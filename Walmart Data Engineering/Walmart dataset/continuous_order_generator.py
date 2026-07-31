"""Generate one coherent Walmart order at a fixed interval.

This script only writes to the existing ``raw`` PostgreSQL tables. It does not
trigger or modify Airflow, Databricks, or dbt.

One customer is selected when the process starts and is reused for every order.
Each order uses a different store from the previous order and selects one active
employee who belongs to that store. The employee is persisted on ``raw.orders``.
The source trigger assigns the monotonic ``change_version`` used by Databricks;
business timestamps remain ordinary event timestamps.
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    import psycopg2
    from psycopg2.extensions import connection as PgConnection
except ImportError as exc:
    raise SystemExit(
        "Missing PostgreSQL driver. Install it with:\n"
        "  .venv\\Scripts\\python.exe -m pip install "
        "-r \"Walmart dataset\\generator_requirements.txt\""
    ) from exc


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_INTERVAL_SECONDS = 60.0
GENERATOR_ADVISORY_LOCK_ID = 2_026_073_001

PAYMENT_METHODS = (
    "Cash",
    "Debit Card",
    "Credit Card",
    "Gift Card",
    "Online",
)

RANDOM = random.SystemRandom()
STOP_REQUESTED = False


@dataclass(frozen=True)
class Customer:
    customer_id: int
    display_name: str


@dataclass(frozen=True)
class StoreEmployee:
    store_id: int
    store_name: str
    employee_id: int
    employee_name: str


@dataclass(frozen=True)
class Product:
    product_id: int
    product_name: str
    unit_price: Decimal


@dataclass(frozen=True)
class GeneratedOrder:
    order_id: int
    order_item_count: int
    total_amount: Decimal
    customer: Customer
    store_employee: StoreEmployee


def read_environment(path: Path) -> dict[str, str]:
    """Read the existing .env file without changing it."""
    last_error: UnicodeError | None = None

    for encoding in ("utf-8-sig", "utf-16"):
        try:
            content = path.read_text(encoding=encoding)
            break
        except UnicodeError as exc:
            last_error = exc
    else:
        raise RuntimeError(f"Cannot decode environment file: {path}") from last_error

    values: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def get_connection_string() -> str:
    process_value = os.getenv("POSTGRES_CONNECTION")
    if process_value:
        return process_value

    if not ENV_FILE.is_file():
        raise RuntimeError(
            "POSTGRES_CONNECTION is not defined and the project .env file "
            f"does not exist: {ENV_FILE}"
        )

    environment = read_environment(ENV_FILE)
    try:
        return environment["POSTGRES_CONNECTION"]
    except KeyError as exc:
        raise RuntimeError(
            f"POSTGRES_CONNECTION is missing from {ENV_FILE}"
        ) from exc


def connect(connection_string: str) -> PgConnection:
    connection = psycopg2.connect(connection_string, connect_timeout=15)
    connection.autocommit = False
    return connection


def utc_now_without_timezone() -> datetime:
    """Return UTC compatible with the existing TIMESTAMP columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def select_customer(
    connection: PgConnection,
    requested_customer_id: int | None,
) -> Customer:
    with connection.cursor() as cursor:
        if requested_customer_id is None:
            cursor.execute(
                """
                SELECT
                    customer_id,
                    CONCAT(first_name, ' ', last_name)
                FROM raw.customers
                WHERE is_active = 'Y'
                ORDER BY RANDOM()
                LIMIT 1
                """
            )
        else:
            cursor.execute(
                """
                SELECT
                    customer_id,
                    CONCAT(first_name, ' ', last_name)
                FROM raw.customers
                WHERE customer_id = %s
                  AND is_active = 'Y'
                """,
                (requested_customer_id,),
            )

        row = cursor.fetchone()

    connection.rollback()

    if row is None:
        if requested_customer_id is None:
            raise RuntimeError("No active customer exists in raw.customers.")
        raise RuntimeError(
            f"Customer {requested_customer_id} does not exist or is not active."
        )

    return Customer(customer_id=int(row[0]), display_name=str(row[1]))


def select_store_and_employee(
    cursor: Any,
    previous_store_id: int | None,
) -> StoreEmployee:
    cursor.execute(
        """
        SELECT
            s.store_id,
            s.store_name,
            e.employee_id,
            CONCAT(e.first_name, ' ', e.last_name)
        FROM raw.stores AS s
        INNER JOIN raw.employees AS e
            ON e.store_id = s.store_id
        WHERE s.is_active = 'Y'
          AND e.is_active = 'Y'
          AND (%s IS NULL OR s.store_id <> %s)
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (previous_store_id, previous_store_id),
    )
    row = cursor.fetchone()

    if row is None and previous_store_id is not None:
        # Allows operation with a database containing only one eligible store.
        return select_store_and_employee(cursor, previous_store_id=None)

    if row is None:
        raise RuntimeError(
            "No active store with at least one active employee exists."
        )

    return StoreEmployee(
        store_id=int(row[0]),
        store_name=str(row[1]),
        employee_id=int(row[2]),
        employee_name=str(row[3]),
    )


def select_products(cursor: Any) -> list[Product]:
    requested_count = RANDOM.randint(1, 5)
    cursor.execute(
        """
        SELECT product_id, product_name, price
        FROM raw.products
        WHERE is_active = 'Y'
          AND price > 0
        ORDER BY RANDOM()
        LIMIT %s
        """,
        (requested_count,),
    )
    rows = cursor.fetchall()

    if not rows:
        raise RuntimeError("No active product with a positive price exists.")

    return [
        Product(
            product_id=int(row[0]),
            product_name=str(row[1]),
            unit_price=Decimal(row[2]),
        )
        for row in rows
    ]


def next_identifiers(cursor: Any, order_item_count: int) -> tuple[int, list[int]]:
    # The advisory transaction lock serializes multiple copies of this generator.
    # Existing source tables do not currently use PostgreSQL sequences.
    cursor.execute(
        "SELECT pg_advisory_xact_lock(%s)",
        (GENERATOR_ADVISORY_LOCK_ID,),
    )
    cursor.execute("SELECT COALESCE(MAX(order_id), 0) + 1 FROM raw.orders")
    order_id = int(cursor.fetchone()[0])

    cursor.execute(
        "SELECT COALESCE(MAX(order_item_id), 0) + 1 FROM raw.order_items"
    )
    first_order_item_id = int(cursor.fetchone()[0])
    order_item_ids = list(
        range(first_order_item_id, first_order_item_id + order_item_count)
    )
    return order_id, order_item_ids


def create_order(
    connection: PgConnection,
    customer: Customer,
    previous_store_id: int | None,
) -> GeneratedOrder:
    try:
        with connection.cursor() as cursor:
            store_employee = select_store_and_employee(
                cursor,
                previous_store_id=previous_store_id,
            )
            products = select_products(cursor)
            order_id, order_item_ids = next_identifiers(
                cursor,
                order_item_count=len(products),
            )

            created_at = utc_now_without_timezone()
            payment_method = RANDOM.choice(PAYMENT_METHODS)
            item_rows: list[tuple[object, ...]] = []
            total_amount = Decimal("0.00")

            for order_item_id, product in zip(
                order_item_ids,
                products,
                strict=True,
            ):
                quantity = RANDOM.randint(1, 5)
                line_amount = (
                    product.unit_price * quantity
                ).quantize(Decimal("0.01"))
                total_amount += line_amount
                item_rows.append(
                    (
                        order_item_id,
                        order_id,
                        product.product_id,
                        quantity,
                        product.unit_price,
                        line_amount,
                        created_at,
                        created_at,
                        "Y",
                    )
                )

            total_amount = total_amount.quantize(Decimal("0.01"))

            cursor.execute(
                """
                INSERT INTO raw.orders (
                    order_id,
                    customer_id,
                    store_id,
                    employee_id,
                    order_timestamp,
                    payment_method,
                    order_status,
                    total_amount,
                    created_timestamp,
                    updated_timestamp,
                    is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    customer.customer_id,
                    store_employee.store_id,
                    store_employee.employee_id,
                    created_at,
                    payment_method,
                    "Completed",
                    total_amount,
                    created_at,
                    created_at,
                    "Y",
                ),
            )

            cursor.executemany(
                """
                INSERT INTO raw.order_items (
                    order_item_id,
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    line_amount,
                    created_timestamp,
                    updated_timestamp,
                    is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                item_rows,
            )

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return GeneratedOrder(
        order_id=order_id,
        order_item_count=len(item_rows),
        total_amount=total_amount,
        customer=customer,
        store_employee=store_employee,
    )


def format_order(order: GeneratedOrder) -> str:
    employee_note = (
        f"employee={order.store_employee.employee_id} "
        f"({order.store_employee.employee_name})"
    )
    return (
        f"order={order.order_id} | "
        f"customer={order.customer.customer_id} ({order.customer.display_name}) | "
        f"store={order.store_employee.store_id} "
        f"({order.store_employee.store_name}) | "
        f"{employee_note} | "
        f"items={order.order_item_count} | "
        f"total={order.total_amount}"
    )


def request_stop(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def positive_interval(value: str) -> float:
    interval = float(value)
    if interval <= 0:
        raise argparse.ArgumentTypeError("interval must be greater than zero")
    return interval


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously create coherent orders for one existing customer."
        )
    )
    parser.add_argument(
        "--interval",
        type=positive_interval,
        default=float(
            os.getenv(
                "GENERATOR_INTERVAL_SECONDS",
                DEFAULT_INTERVAL_SECONDS,
            )
        ),
        help="Seconds between orders (default: 60).",
    )
    parser.add_argument(
        "--customer-id",
        type=int,
        default=(
            int(os.environ["GENERATOR_CUSTOMER_ID"])
            if os.getenv("GENERATOR_CUSTOMER_ID")
            else None
        ),
        help=(
            "Existing active customer reused for every order. "
            "If omitted, one active customer is selected once at startup."
        ),
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Create exactly one order and exit.",
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    connection_string = get_connection_string()
    connection = connect(connection_string)

    try:
        customer = select_customer(
            connection,
            requested_customer_id=arguments.customer_id,
        )
        print(
            f"Fixed customer: {customer.customer_id} "
            f"({customer.display_name})"
        )
        print(
            f"Generation interval: {arguments.interval:g} seconds. "
            "Press Ctrl+C to stop."
        )

        previous_store_id: int | None = None
        next_run_at = time.monotonic()

        while not STOP_REQUESTED:
            try:
                order = create_order(
                    connection,
                    customer=customer,
                    previous_store_id=previous_store_id,
                )
                previous_store_id = order.store_employee.store_id
                print(
                    f"{utc_now_without_timezone().isoformat()}Z | "
                    f"{format_order(order)}",
                    flush=True,
                )
            except psycopg2.Error as exc:
                connection.rollback()
                print(
                    f"PostgreSQL error; this interval was skipped: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                if connection.closed:
                    connection = connect(connection_string)
            except Exception as exc:
                connection.rollback()
                print(
                    f"Generation error; this interval was skipped: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

            if arguments.once:
                break

            next_run_at += arguments.interval
            remaining_seconds = max(0.0, next_run_at - time.monotonic())
            if remaining_seconds:
                time.sleep(remaining_seconds)
    finally:
        connection.close()

    print("Generator stopped cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
