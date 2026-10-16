"""Rebuild the local raw PostgreSQL source from a Databricks bronze schema.

Use it once after the local source container is (re)created, so that order
identifiers and ``change_version`` cursors stay aligned with what Databricks
has already ingested. The local ``raw`` tables are truncated and replaced.

    python restore_source_from_bronze.py --source-schema bronze_local --apply

Credentials come from ``DATABRICKS_HOST``, ``DATABRICKS_TOKEN`` and
``DATABRICKS_HTTP_PATH`` (environment or airflow/.env). The local database URL
comes from ``WALMART_SOURCE_DB_URL`` (default: localhost:5433).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from databricks import sql


AIRFLOW_ENV = Path(__file__).resolve().parents[2] / ".env"
DEFAULT_SOURCE_URL = "postgresql://walmart:walmart@localhost:5433/walmart"
# Parents first so foreign keys remain satisfied.
TABLES = ("customers", "stores", "products", "employees", "orders", "order_items")


def setting(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    if AIRFLOW_ENV.is_file():
        for line in AIRFLOW_ENV.read_text(encoding="utf-8-sig").splitlines():
            key, _, raw_value = line.partition("=")
            if key.strip() == name:
                return raw_value.strip()
    raise RuntimeError(f"{name} is not defined")


def restore(source_schema: str, apply: bool) -> None:
    source_url = os.getenv("WALMART_SOURCE_DB_URL", DEFAULT_SOURCE_URL)

    with sql.connect(
        server_hostname=setting("DATABRICKS_HOST").removeprefix("https://"),
        http_path=setting("DATABRICKS_HTTP_PATH"),
        access_token=setting("DATABRICKS_TOKEN"),
    ) as databricks:
        cursor = databricks.cursor()
        snapshot: dict[str, tuple[list[str], list[tuple]]] = {}
        for table in TABLES:
            cursor.execute(f"SELECT * FROM walmart.{source_schema}.{table}")
            columns = [column[0] for column in cursor.description]
            rows = [tuple(row) for row in cursor.fetchall()]
            snapshot[table] = (columns, rows)
            print(f"walmart.{source_schema}.{table}: {len(rows)} rows")

    if not apply:
        print("Dry run only. Pass --apply to replace the local raw tables.")
        return

    connection = psycopg2.connect(source_url, connect_timeout=15)
    try:
        with connection.cursor() as pg:
            # Keep the bronze change_version values instead of letting the
            # source trigger assign new ones.
            pg.execute("SET session_replication_role = replica")
            pg.execute(
                "TRUNCATE "
                + ", ".join(f"raw.{table}" for table in reversed(TABLES))
            )
            for table in TABLES:
                columns, rows = snapshot[table]
                execute_values(
                    pg,
                    f"INSERT INTO raw.{table} ({', '.join(columns)}) VALUES %s",
                    rows,
                    page_size=2000,
                )
            pg.execute(
                """
                SELECT setval(
                    'raw.walmart_change_version_seq',
                    GREATEST(
                        (SELECT COALESCE(MAX(change_version), 0) FROM raw.orders),
                        (SELECT COALESCE(MAX(change_version), 0) FROM raw.order_items),
                        1
                    )
                )
                """
            )
            print(f"change_version sequence set to {pg.fetchone()[0]}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    print("Local raw source restored.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-schema", default="bronze_local")
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args()
    restore(arguments.source_schema, arguments.apply)
