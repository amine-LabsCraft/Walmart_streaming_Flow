"""Incremental push of the local Walmart source into Databricks bronze.

Databricks cannot reach the PostgreSQL container running on the local machine,
so the Airflow worker reads the source and writes the changed rows itself.

Each table is synchronised with a watermark read from its bronze copy:
orders and order_items use the monotonic ``change_version`` cursor, the other
tables use ``updated_timestamp``. Rows are upserted on their primary key, so a
retried run is idempotent.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

import psycopg2
from databricks import sql


# table -> (primary key, cursor column, bronze column definitions)
TABLES: dict[str, tuple[str, str, str]] = {
    "customers": (
        "customer_id",
        "updated_timestamp",
        "customer_id BIGINT, first_name VARCHAR(100), last_name VARCHAR(100), "
        "email VARCHAR(255), phone VARCHAR(50), city VARCHAR(100), "
        "province VARCHAR(100), country VARCHAR(100), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, "
        "is_active CHAR(1)",
    ),
    "stores": (
        "store_id",
        "updated_timestamp",
        "store_id BIGINT, store_name VARCHAR(255), city VARCHAR(100), "
        "province VARCHAR(100), country VARCHAR(100), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, "
        "is_active CHAR(1)",
    ),
    "products": (
        "product_id",
        "updated_timestamp",
        "product_id BIGINT, product_name VARCHAR(255), category VARCHAR(100), "
        "brand VARCHAR(100), price DECIMAL(10,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, "
        "is_active CHAR(1)",
    ),
    "employees": (
        "employee_id",
        "updated_timestamp",
        "employee_id BIGINT, store_id BIGINT, first_name VARCHAR(100), "
        "last_name VARCHAR(100), email VARCHAR(255), job_title VARCHAR(100), "
        "salary DECIMAL(10,2), created_timestamp TIMESTAMP, "
        "updated_timestamp TIMESTAMP, is_active CHAR(1)",
    ),
    "orders": (
        "order_id",
        "change_version",
        "order_id BIGINT, customer_id BIGINT, store_id BIGINT, "
        "order_timestamp TIMESTAMP, payment_method VARCHAR(50), "
        "order_status VARCHAR(50), total_amount DECIMAL(12,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, "
        "is_active CHAR(1), employee_id BIGINT, change_version BIGINT",
    ),
    "order_items": (
        "order_item_id",
        "change_version",
        "order_item_id BIGINT, order_id BIGINT, product_id BIGINT, "
        "quantity INT, unit_price DECIMAL(10,2), line_amount DECIMAL(12,2), "
        "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP, "
        "is_active CHAR(1), change_version BIGINT",
    ),
}
BATCH_SIZE = 1000


def sql_literal(value: Any) -> str:
    """Render a PostgreSQL value as a Databricks SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.replace(tzinfo=None).isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    raise TypeError(f"Unsupported source value type: {type(value).__name__}")


def column_casts(definition: str) -> list[tuple[str, str]]:
    """Return (column, cast type) pairs; CHAR/VARCHAR are cast to STRING."""
    pairs = []
    for column_definition in definition.split(", "):
        name, column_type = column_definition.split(" ", 1)
        if column_type.startswith(("VARCHAR", "CHAR")):
            column_type = "STRING"
        pairs.append((name, column_type))
    return pairs


def batches(rows: list[tuple], size: int) -> Iterable[list[tuple]]:
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def sync_source_to_bronze(
    source_url: str,
    server_hostname: str,
    http_path: str,
    access_token: str,
    catalog: str = "walmart",
    schema: str = "bronze_local",
) -> dict[str, int]:
    """Copy new or changed source rows into ``catalog.schema``."""
    synced: dict[str, int] = {}
    source = psycopg2.connect(source_url, connect_timeout=15)
    source.set_session(readonly=True)

    try:
        with sql.connect(
            server_hostname=server_hostname.removeprefix("https://"),
            http_path=http_path,
            access_token=access_token,
        ) as databricks:
            target_cursor = databricks.cursor()
            target_cursor.execute(
                f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
            )

            for table, (key, cursor_column, definition) in TABLES.items():
                target = f"{catalog}.{schema}.{table}"
                target_cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {target} ({definition})"
                )
                target_cursor.execute(
                    f"SELECT MAX({cursor_column}) FROM {target}"
                )
                watermark = target_cursor.fetchone()[0]

                casts = column_casts(definition)
                columns = [name for name, _ in casts]
                query = f"SELECT {', '.join(columns)} FROM raw.{table}"
                parameters: tuple = ()
                if watermark is not None:
                    # Timestamps use >= so rows sharing the watermark are not
                    # lost; the MERGE makes the overlap harmless.
                    operator = ">" if cursor_column == "change_version" else ">="
                    query += f" WHERE {cursor_column} {operator} %s"
                    if isinstance(watermark, datetime):
                        watermark = watermark.replace(tzinfo=None)
                    parameters = (watermark,)
                query += f" ORDER BY {cursor_column}"

                with source.cursor() as source_cursor:
                    source_cursor.execute(query, parameters)
                    rows = source_cursor.fetchall()
                source.rollback()

                select_list = ", ".join(
                    f"CAST({name} AS {cast_type}) AS {name}"
                    for name, cast_type in casts
                )
                for batch in batches(rows, BATCH_SIZE):
                    values = ",\n".join(
                        "(" + ", ".join(sql_literal(value) for value in row) + ")"
                        for row in batch
                    )
                    staged = (
                        f"SELECT {select_list} FROM VALUES {values} "
                        f"AS source({', '.join(columns)})"
                    )
                    if watermark is None:
                        statement = f"INSERT INTO {target} {staged}"
                    else:
                        statement = (
                            f"MERGE INTO {target} AS target "
                            f"USING ({staged}) AS source "
                            f"ON target.{key} = source.{key} "
                            "WHEN MATCHED THEN UPDATE SET * "
                            "WHEN NOT MATCHED THEN INSERT *"
                        )
                    target_cursor.execute(statement)

                synced[table] = len(rows)
                print(f"{target}: {len(rows)} row(s) synchronised")
    finally:
        source.close()

    return synced
