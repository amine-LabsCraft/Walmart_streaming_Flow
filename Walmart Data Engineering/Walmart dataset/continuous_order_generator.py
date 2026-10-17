"""Generate coherent Walmart Canada supercentre activity at a fixed interval.

This script only writes to the existing ``raw`` PostgreSQL tables. It does not
trigger or modify Airflow, Databricks, or dbt.

Every tick creates exactly one order following ``business_rules.py``:

* while stores are open (07:00-23:00 local time) most orders are in-store
  sales, paid at the till and handled by a cashier; online orders arrive
  around the clock and are prepared by an online order picker;
* the store is chosen according to its current footfall, the customer
  usually lives near it (home store) and loyal customers shop more often;
* the basket mixes groceries, household items and general merchandise with
  category-specific quantities, at the current catalogue price.

After each order the store lifecycle moves on: pending online orders are
completed (or cancelled), a recent sale is sometimes returned and a product
price occasionally changes. These are UPDATEs: they never create orders, so
one generated order always counts as exactly one order downstream.

The source trigger assigns the monotonic ``change_version`` used by the
ingestion; ``order_timestamp`` is the store's local time, while technical
``created_timestamp`` / ``updated_timestamp`` values are UTC.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import random
import signal
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

import business_rules as rules


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_CONNECTION_STRING = "postgresql://walmart:walmart@localhost:5433/walmart"
DEFAULT_INTERVAL_SECONDS = 60.0
GENERATOR_ADVISORY_LOCK_ID = 2_026_073_001

RANDOM = random.SystemRandom()
STOP_REQUESTED = False

# Popularity of each catalogue product, by product_id (catalogue order).
PRODUCT_POPULARITY = {
    product_id: item.popularity
    for product_id, item in enumerate(rules.build_catalog(), start=1)
}

CUSTOMER_PICK_SQL = f"""
    SELECT customer_id, CONCAT(first_name, ' ', last_name)
    FROM raw.customers
    WHERE is_active = 'Y' AND {{scope}}
    ORDER BY -ln(1.0 - random())
        / ({rules.LOYALTY_WEIGHT_SQL.format(column='customer_id')}
           * CASE WHEN created_timestamp > %s THEN {rules.NEW_CUSTOMER_BOOST} ELSE 1 END)
    LIMIT 1
"""


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
    category: str
    unit_price: Decimal


@dataclass(frozen=True)
class GeneratedOrder:
    order_id: int
    order_item_count: int
    total_amount: Decimal
    customer: Customer
    store_employee: StoreEmployee
    channel: str
    status: str
    payment_method: str


@dataclass(frozen=True)
class LifecycleUpdate:
    completed: int = 0
    cancelled: int = 0
    returned: int = 0
    repriced: int = 0


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

    if ENV_FILE.is_file():
        environment = read_environment(ENV_FILE)
        if environment.get("POSTGRES_CONNECTION"):
            return environment["POSTGRES_CONNECTION"]

    # Local walmart-source-db container from airflow/docker-compose.yaml.
    return DEFAULT_CONNECTION_STRING


def connect(connection_string: str) -> PgConnection:
    connection = psycopg2.connect(connection_string, connect_timeout=15)
    connection.autocommit = False
    return connection


@contextlib.contextmanager
def keep_system_awake() -> Iterator[None]:
    """Prevent Windows idle sleep while orders are being generated.

    Modern Standby cuts the network and freezes Docker, which stalls the
    Airflow tasks talking to Databricks. This only blocks *idle* sleep for the
    calling thread (like a media player); closing the lid or the power button
    still work, and the request ends with the block. No-op on other systems.
    """
    if sys.platform != "win32":
        yield
        return
    import ctypes

    es_continuous, es_system_required = 0x80000000, 0x00000001
    kernel32 = ctypes.windll.kernel32
    kernel32.SetThreadExecutionState(es_continuous | es_system_required)
    try:
        yield
    finally:
        kernel32.SetThreadExecutionState(es_continuous)


def utc_now_without_timezone() -> datetime:
    """Return UTC compatible with the existing TIMESTAMP columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Selection helpers
# ---------------------------------------------------------------------------

def choose_channel_and_store(now_utc: datetime) -> tuple[str, rules.StoreProfile]:
    """In-store while stores are open (by footfall), online otherwise."""
    aware_now = now_utc.replace(tzinfo=timezone.utc)
    in_store_weights: dict[int, float] = {}
    online_weights: dict[int, float] = {}
    for store in rules.STORES.values():
        local = rules.local_now(store, aware_now)
        day = rules.day_factor(local.date())
        online_weights[store.store_id] = (
            store.traffic_weight * rules.ONLINE_HOURLY[local.hour] * day
        )
        if rules.is_open(local.hour):
            in_store_weights[store.store_id] = (
                store.traffic_weight * rules.IN_STORE_HOURLY[local.hour] * day
            )

    if in_store_weights and RANDOM.random() >= rules.ONLINE_SHARE:
        return "In-store", rules.STORES[rules.weighted_choice(RANDOM, in_store_weights)]
    return "Online", rules.STORES[rules.weighted_choice(RANDOM, online_weights)]


def select_customer(
    connection: PgConnection,
    requested_customer_id: int | None,
) -> Customer:
    """Return the requested customer, or any active customer (by loyalty)."""
    with connection.cursor() as cursor:
        if requested_customer_id is None:
            cursor.execute(
                CUSTOMER_PICK_SQL.format(scope="TRUE"),
                (utc_now_without_timezone() - timedelta(hours=48),),
            )
        else:
            cursor.execute(
                """
                SELECT customer_id, CONCAT(first_name, ' ', last_name)
                FROM raw.customers
                WHERE customer_id = %s AND is_active = 'Y'
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


def select_customer_for_store(cursor: Any, store: rules.StoreProfile) -> Customer:
    """Mostly a neighbour of the store; loyal and new customers shop more."""
    draw = RANDOM.random()
    scopes = []
    if draw < rules.HOME_STORE_SHARE:
        scopes.append(("city = %s", store.city))
    if draw < rules.HOME_STORE_SHARE + rules.SAME_PROVINCE_SHARE:
        scopes.append(("province = %s", store.province))
    scopes.append(("TRUE", None))

    new_since = utc_now_without_timezone() - timedelta(hours=48)
    for scope, value in scopes:
        parameters = (value, new_since) if value is not None else (new_since,)
        cursor.execute(CUSTOMER_PICK_SQL.format(scope=scope), parameters)
        row = cursor.fetchone()
        if row:
            return Customer(customer_id=int(row[0]), display_name=str(row[1]))
    raise RuntimeError("No active customer exists in raw.customers.")


def home_store_of(cursor: Any, customer: Customer) -> rules.StoreProfile:
    cursor.execute("SELECT city, province FROM raw.customers WHERE customer_id = %s",
                   (customer.customer_id,))
    city, province = cursor.fetchone()
    for store in rules.STORES.values():
        if store.city == city:
            return store
    for store in rules.STORES.values():
        if store.province == province:
            return store
    return rules.STORES[1]


def select_employee(cursor: Any, store: rules.StoreProfile, channel: str) -> StoreEmployee:
    """A cashier rings in-store sales; an online picker prepares web orders."""
    titles = (rules.PICKER_TITLE,) if channel == "Online" else rules.CASHIER_TITLES
    for title_filter in ("AND e.job_title = ANY(%s)", ""):
        parameters: tuple = (store.store_id, list(titles)) if title_filter else (store.store_id,)
        cursor.execute(
            f"""
            SELECT s.store_id, s.store_name, e.employee_id,
                   CONCAT(e.first_name, ' ', e.last_name)
            FROM raw.stores AS s
            JOIN raw.employees AS e ON e.store_id = s.store_id
            WHERE s.store_id = %s AND s.is_active = 'Y' AND e.is_active = 'Y'
              {title_filter}
            ORDER BY random()
            LIMIT 1
            """,
            parameters,
        )
        row = cursor.fetchone()
        if row:
            return StoreEmployee(
                store_id=int(row[0]),
                store_name=str(row[1]),
                employee_id=int(row[2]),
                employee_name=str(row[3]),
            )
    raise RuntimeError(f"No active employee in store {store.store_id}.")


def select_basket(cursor: Any, channel: str) -> list[tuple[Product, int]]:
    """Distinct products, weighted by category share and popularity."""
    cursor.execute(
        """
        SELECT product_id, product_name, category, price
        FROM raw.products
        WHERE is_active = 'Y' AND price > 0
        """
    )
    by_category: dict[str, list[Product]] = {}
    for product_id, name, category, price in cursor.fetchall():
        by_category.setdefault(category, []).append(
            Product(int(product_id), str(name), str(category), Decimal(price))
        )
    if not by_category:
        raise RuntimeError("No active product with a positive price exists.")

    category_weights = {
        category: weight
        for category, weight in rules.CATEGORY_LINE_WEIGHTS.items()
        if category in by_category
    }
    line_weights = rules.ONLINE_LINE_COUNTS if channel == "Online" else rules.IN_STORE_LINE_COUNTS
    line_count = rules.weighted_choice(RANDOM, line_weights)

    basket: list[tuple[Product, int]] = []
    chosen: set[int] = set()
    for _ in range(line_count * 10):
        if len(basket) == line_count:
            break
        category = rules.weighted_choice(RANDOM, category_weights)
        candidates = by_category[category]
        product = RANDOM.choices(
            candidates,
            weights=[
                rules.product_weight(p.product_id, PRODUCT_POPULARITY.get(p.product_id, 1.0))
                for p in candidates
            ],
            k=1,
        )[0]
        if product.product_id in chosen:
            continue
        chosen.add(product.product_id)
        quantity = rules.weighted_choice(RANDOM, rules.QUANTITY_WEIGHTS.get(category, {1: 1}))
        basket.append((product, quantity))
    return basket


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


# ---------------------------------------------------------------------------
# Order creation
# ---------------------------------------------------------------------------

def create_order(
    connection: PgConnection,
    customer: Customer | None = None,
) -> GeneratedOrder:
    """Insert exactly one order and its lines in a single transaction."""
    try:
        with connection.cursor() as cursor:
            now_utc = utc_now_without_timezone()
            if customer is None:
                channel, store = choose_channel_and_store(now_utc)
                customer = select_customer_for_store(cursor, store)
            else:
                store = home_store_of(cursor, customer)
                local_hour = rules.local_now(store, now_utc.replace(tzinfo=timezone.utc)).hour
                channel = "In-store" if rules.is_open(local_hour) else "Online"

            store_employee = select_employee(cursor, store, channel)
            basket = select_basket(cursor, channel)
            order_id, order_item_ids = next_identifiers(cursor, order_item_count=len(basket))

            order_timestamp = rules.local_now(
                store, now_utc.replace(tzinfo=timezone.utc)
            ).replace(tzinfo=None)
            if channel == "Online":
                payment_method, status = rules.ONLINE_PAYMENT, "Pending"
            else:
                payment_method = rules.weighted_choice(RANDOM, rules.IN_STORE_PAYMENTS)
                status = "Completed"

            item_rows: list[tuple[object, ...]] = []
            total_amount = Decimal("0.00")
            for order_item_id, (product, quantity) in zip(order_item_ids, basket, strict=True):
                line_amount = rules.money(product.unit_price * quantity)
                total_amount += line_amount
                item_rows.append(
                    (
                        order_item_id,
                        order_id,
                        product.product_id,
                        quantity,
                        product.unit_price,
                        line_amount,
                        now_utc,
                        now_utc,
                        "Y",
                    )
                )
            total_amount = rules.money(total_amount)

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
                    order_timestamp,
                    payment_method,
                    status,
                    total_amount,
                    now_utc,
                    now_utc,
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
        channel=channel,
        status=status,
        payment_method=payment_method,
    )


# ---------------------------------------------------------------------------
# Store lifecycle
# ---------------------------------------------------------------------------

def advance_lifecycle(connection: PgConnection) -> LifecycleUpdate:
    """Settle online orders, register returns and occasional price changes.

    Only UPDATEs: the number of orders never changes here.
    """
    completed = cancelled = returned = repriced = 0
    try:
        with connection.cursor() as cursor:
            now_utc = utc_now_without_timezone()

            # Online orders are prepared within a few minutes, a few are cancelled.
            cursor.execute(
                """
                SELECT order_id FROM raw.orders
                WHERE order_status = 'Pending' AND payment_method = %s
                  AND created_timestamp < %s
                FOR UPDATE SKIP LOCKED
                """,
                (rules.ONLINE_PAYMENT, now_utc - timedelta(minutes=rules.ONLINE_PREPARATION_MINUTES)),
            )
            for (order_id,) in cursor.fetchall():
                status = "Cancelled" if RANDOM.random() < rules.ONLINE_CANCEL_RATE else "Completed"
                cursor.execute(
                    "UPDATE raw.orders SET order_status = %s, updated_timestamp = %s WHERE order_id = %s",
                    (status, now_utc, order_id),
                )
                if status == "Completed":
                    completed += 1
                else:
                    cancelled += 1

            # Customers bring back a recent purchase now and then.
            if RANDOM.random() < rules.IN_STORE_RETURN_RATE:
                cursor.execute(
                    """
                    UPDATE raw.orders SET order_status = 'Returned', updated_timestamp = %s
                    WHERE order_id = (
                        SELECT order_id FROM raw.orders
                        WHERE order_status = 'Completed'
                          AND created_timestamp BETWEEN %s AND %s
                        ORDER BY random() LIMIT 1
                    )
                    """,
                    (now_utc, now_utc - timedelta(days=rules.RETURN_WINDOW_DAYS),
                     now_utc - timedelta(days=1)),
                )
                returned = cursor.rowcount

            # Rollbacks and supplier price increases change the catalogue.
            if RANDOM.random() < rules.PRICE_CHANGE_PROBABILITY:
                cursor.execute(
                    "SELECT product_id, price FROM raw.products WHERE is_active = 'Y' "
                    "ORDER BY random() LIMIT 1"
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        "UPDATE raw.products SET price = %s, updated_timestamp = %s WHERE product_id = %s",
                        (rules.new_price(Decimal(row[1]), RANDOM), now_utc, row[0]),
                    )
                    repriced = 1
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return LifecycleUpdate(completed, cancelled, returned, repriced)


def format_order(order: GeneratedOrder) -> str:
    employee_note = (
        f"employee={order.store_employee.employee_id} "
        f"({order.store_employee.employee_name})"
    )
    return (
        f"order={order.order_id} | {order.channel} | {order.status} | "
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
        description="Continuously create coherent supercentre orders."
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
            "Existing active customer reused for every order (at their home "
            "store). If omitted, customers follow the business rules."
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
        customer = None
        if arguments.customer_id is not None:
            customer = select_customer(connection, arguments.customer_id)
            print(f"Fixed customer: {customer.customer_id} ({customer.display_name})")
        print(
            f"Generation interval: {arguments.interval:g} seconds. "
            "Press Ctrl+C to stop."
        )

        next_run_at = time.monotonic()

        with keep_system_awake():
            while not STOP_REQUESTED:
                try:
                    order = create_order(connection, customer=customer)
                    lifecycle = advance_lifecycle(connection)
                    print(
                        f"{utc_now_without_timezone().isoformat()}Z | "
                        f"{format_order(order)} | lifecycle={lifecycle}",
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
