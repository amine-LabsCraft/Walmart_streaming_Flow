"""Read-only Prometheus exporter for Walmart pipeline freshness and quality."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg2
import requests
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily

LOGGER = logging.getLogger("walmart-exporter")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def as_epoch(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    return float(value)


def enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class CacheEntry:
    collected_at: float
    values: dict[str, float]


class CachedCollector:
    def __init__(self) -> None:
        self._cache_seconds = float(
            os.getenv("WALMART_EXPORTER_CACHE_SECONDS", "30")
        )
        self._lock = threading.Lock()
        self._cache: dict[str, CacheEntry] = {}

    def _cached(
        self,
        name: str,
        loader: Callable[[], dict[str, float]],
    ) -> dict[str, float]:
        with self._lock:
            cached = self._cache.get(name)
            now = time.monotonic()
            if cached and now - cached.collected_at < self._cache_seconds:
                return cached.values

        values = loader()
        with self._lock:
            self._cache[name] = CacheEntry(time.monotonic(), values)
        return values

    def _source_values(self) -> dict[str, float]:
        dsn = os.getenv("POSTGRES_CONNECTION")
        if not dsn:
            return {"up": 0.0}

        started = time.monotonic()
        try:
            with psycopg2.connect(dsn, connect_timeout=5) as connection:
                connection.set_session(readonly=True, autocommit=True)
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            COUNT(*)::bigint,
                            MAX(orders.order_id)::bigint,
                            MAX(orders.order_timestamp),
                            GREATEST(
                                MAX(GREATEST(
                                    orders.created_timestamp,
                                    orders.updated_timestamp
                                )),
                                (SELECT MAX(GREATEST(
                                    items.created_timestamp,
                                    items.updated_timestamp
                                )) FROM raw.order_items AS items),
                                (SELECT MAX(GREATEST(
                                    customers.created_timestamp,
                                    customers.updated_timestamp
                                )) FROM raw.customers AS customers)
                            ) AS last_source_change,
                            COUNT(*) FILTER (
                                WHERE orders.order_timestamp > CURRENT_TIMESTAMP
                            )::bigint AS future_orders
                        FROM raw.orders AS orders
                        """
                    )
                    (
                        order_count,
                        max_order_id,
                        last_order,
                        last_source_change,
                        future_orders,
                    ) = cursor.fetchone()

                    cursor.execute("SELECT COUNT(*)::bigint FROM raw.order_items")
                    order_item_count = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*)::bigint FROM raw.customers")
                    customer_count = cursor.fetchone()[0]

                    cursor.execute(
                        """
                        SELECT COUNT(*)::bigint
                        FROM raw.orders AS orders
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM raw.order_items AS items
                            WHERE items.order_id = orders.order_id
                        )
                        """
                    )
                    orders_without_items = cursor.fetchone()[0]

            return {
                "up": 1.0,
                "orders": float(order_count or 0),
                "order_items": float(order_item_count or 0),
                "customers": float(customer_count or 0),
                "max_order_id": float(max_order_id or 0),
                "last_order": as_epoch(last_order),
                "last_source_change": as_epoch(last_source_change),
                "future_orders": float(future_orders or 0),
                "orders_without_items": float(orders_without_items or 0),
                "duration": time.monotonic() - started,
            }
        except Exception:
            LOGGER.exception("Ghost PostgreSQL metrics collection failed")
            return {"up": 0.0, "duration": time.monotonic() - started}

    def _airflow_values(self) -> dict[str, float]:
        dsn = os.getenv("AIRFLOW_DB_CONNECTION")
        if not dsn:
            return {"up": 0.0}

        try:
            with psycopg2.connect(dsn, connect_timeout=5) as connection:
                connection.set_session(readonly=True, autocommit=True)
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            MAX(end_date) FILTER (WHERE state = 'success'),
                            MAX(end_date) FILTER (WHERE state = 'failed'),
                            COUNT(*) FILTER (WHERE state = 'running')
                        FROM dag_run
                        WHERE dag_id = 'orchestrate'
                        """
                    )
                    last_success, last_failure, running = cursor.fetchone()
            return {
                "up": 1.0,
                "last_success": as_epoch(last_success),
                "last_failure": as_epoch(last_failure),
                "running": float(running or 0),
            }
        except Exception:
            LOGGER.exception("Airflow metadata metrics collection failed")
            return {"up": 0.0}

    def _gold_values(self) -> dict[str, float]:
        if not enabled("WALMART_DATABRICKS_METRICS_ENABLED"):
            return {"enabled": 0.0}

        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        table = os.getenv("WALMART_GOLD_TABLE", "walmart.gold.fact_orders")
        if not host or not token or not http_path:
            return {"enabled": 1.0, "up": 0.0}

        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.$")
        if not table or any(character not in allowed for character in table):
            LOGGER.error("WALMART_GOLD_TABLE contains unsupported characters")
            return {"enabled": 1.0, "up": 0.0}

        started = time.monotonic()
        try:
            warehouse_marker = "/warehouses/"
            if warehouse_marker not in http_path:
                raise ValueError(
                    "DATABRICKS_HTTP_PATH must reference a SQL warehouse"
                )
            warehouse_id = http_path.rsplit(warehouse_marker, 1)[1].strip("/")
            base_url = host.rstrip("/")
            if not base_url.startswith(("http://", "https://")):
                base_url = f"https://{base_url}"

            statement = f"""
                SELECT
                    COUNT(*) AS row_count,
                    unix_timestamp(MAX(order_timestamp)) AS last_order,
                    unix_timestamp(MAX(processed_at)) AS last_processed,
                    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END)
                        AS null_order_items
                FROM {table}
            """
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "walmart-observability-exporter/1.0",
            }
            response = requests.post(
                f"{base_url}/api/2.0/sql/statements/",
                headers=headers,
                json={
                    "warehouse_id": warehouse_id,
                    "statement": statement,
                    "wait_timeout": "10s",
                    "on_wait_timeout": "CONTINUE",
                    "disposition": "INLINE",
                    "format": "JSON_ARRAY",
                },
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            statement_id = payload.get("statement_id")
            deadline = time.monotonic() + float(
                os.getenv("DATABRICKS_QUERY_TIMEOUT_SECONDS", "45")
            )

            state = payload.get("status", {}).get("state")
            while state in {"PENDING", "RUNNING"}:
                if time.monotonic() >= deadline:
                    if statement_id:
                        requests.post(
                            f"{base_url}/api/2.0/sql/statements/{statement_id}/cancel",
                            headers=headers,
                            timeout=10,
                        )
                    raise TimeoutError("Databricks monitoring query timed out")
                time.sleep(2)
                response = requests.get(
                    f"{base_url}/api/2.0/sql/statements/{statement_id}",
                    headers=headers,
                    timeout=15,
                )
                response.raise_for_status()
                payload = response.json()
                state = payload.get("status", {}).get("state")

            if state != "SUCCEEDED":
                error = payload.get("status", {}).get("error", {})
                raise RuntimeError(
                    f"Databricks statement ended in {state}: "
                    f"{error.get('message', 'unknown error')}"
                )

            rows = payload.get("result", {}).get("data_array", [])
            if not rows:
                raise RuntimeError("Databricks Gold query returned no row")
            row_count, last_order, last_processed, null_items = rows[0]
            return {
                "enabled": 1.0,
                "up": 1.0,
                "rows": float(row_count or 0),
                "last_order": float(last_order or 0),
                "last_processed": float(last_processed or 0),
                "null_order_items": float(null_items or 0),
                "duration": time.monotonic() - started,
            }
        except Exception:
            LOGGER.exception("Databricks Gold metrics collection failed")
            return {
                "enabled": 1.0,
                "up": 0.0,
                "duration": time.monotonic() - started,
            }

    @staticmethod
    def gauge(name: str, help_text: str, value: float) -> GaugeMetricFamily:
        metric = GaugeMetricFamily(name, help_text)
        metric.add_metric([], value)
        return metric

    def collect(self):
        source = self._cached("source", self._source_values)
        yield self.gauge("walmart_source_up", "Ghost source connection status.", source.get("up", 0.0))
        yield self.gauge("walmart_source_orders", "Rows in raw.orders.", source.get("orders", 0.0))
        yield self.gauge("walmart_source_order_items", "Rows in raw.order_items.", source.get("order_items", 0.0))
        yield self.gauge("walmart_source_customers", "Rows in raw.customers.", source.get("customers", 0.0))
        yield self.gauge("walmart_source_max_order_id", "Highest source order identifier.", source.get("max_order_id", 0.0))
        yield self.gauge("walmart_source_last_order_timestamp_seconds", "Newest source order timestamp.", source.get("last_order", 0.0))
        yield self.gauge("walmart_source_last_change_timestamp_seconds", "Newest source insert or update timestamp.", source.get("last_source_change", 0.0))
        yield self.gauge("walmart_source_future_orders", "Source orders whose business timestamp is in the future.", source.get("future_orders", 0.0))
        yield self.gauge("walmart_source_orders_without_items", "Orders without matching order items.", source.get("orders_without_items", 0.0))
        yield self.gauge("walmart_source_collection_duration_seconds", "Source collection duration.", source.get("duration", 0.0))

        airflow = self._cached("airflow", self._airflow_values)
        yield self.gauge("walmart_airflow_metadata_up", "Airflow metadata connection status.", airflow.get("up", 0.0))
        yield self.gauge("walmart_airflow_last_success_timestamp_seconds", "Last successful orchestrate DAG run.", airflow.get("last_success", 0.0))
        yield self.gauge("walmart_airflow_last_failure_timestamp_seconds", "Last failed orchestrate DAG run.", airflow.get("last_failure", 0.0))
        yield self.gauge("walmart_airflow_running_dag_runs", "Currently running orchestrate DAG runs.", airflow.get("running", 0.0))

        gold = self._cached("gold", self._gold_values)
        yield self.gauge("walmart_gold_monitoring_enabled", "Whether Databricks SQL monitoring is enabled.", gold.get("enabled", 0.0))
        if gold.get("enabled", 0.0):
            yield self.gauge("walmart_gold_up", "Databricks Gold query status.", gold.get("up", 0.0))
            yield self.gauge("walmart_gold_fact_rows", "Rows in the monitored Gold fact.", gold.get("rows", 0.0))
            yield self.gauge("walmart_gold_last_order_timestamp_seconds", "Newest business event in Gold.", gold.get("last_order", 0.0))
            yield self.gauge("walmart_gold_last_processed_timestamp_seconds", "Newest Gold processing timestamp.", gold.get("last_processed", 0.0))
            yield self.gauge("walmart_gold_null_order_item_rows", "Gold rows with null order_item_id.", gold.get("null_order_items", 0.0))
            yield self.gauge("walmart_gold_collection_duration_seconds", "Databricks collection duration.", gold.get("duration", 0.0))


def main() -> None:
    port = int(os.getenv("WALMART_EXPORTER_PORT", "9105"))
    REGISTRY.register(CachedCollector())
    start_http_server(port)
    LOGGER.info("Walmart exporter listening on port %s", port)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
