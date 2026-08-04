"""Local web interface for the Ghost PostgreSQL source-event generator."""

from __future__ import annotations

import json
import os
import random
import socket
import threading
import unicodedata
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, render_template, request
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from continuous_order_generator import (
    GeneratedOrder,
    connect,
    create_order,
    get_connection_string,
    select_customer,
)


DEFAULT_INTERVAL_SECONDS = 10.0
MIN_INTERVAL_SECONDS = 2.0
MAX_INTERVAL_SECONDS = 3600.0
MAX_ACTIVITY_ITEMS = 40
DEFAULT_PORT = 5050
CUSTOMER_ADVISORY_LOCK_ID = 2_026_073_003
RANDOM = random.SystemRandom()
GENERATOR_MODES = ("idle", "starting", "running", "stopping", "error")

ORDER_EVENTS = Counter(
    "walmart_generator_orders_total",
    "Total number of orders generated during the current process lifetime.",
)
SIGNUP_EVENTS = Counter(
    "walmart_generator_signups_total",
    "Total number of generated customer signups.",
)
GENERATOR_ERRORS = Counter(
    "walmart_generator_errors_total",
    "Total number of generator errors by event type.",
    ("event_type",),
)
GENERATOR_MODE = Gauge(
    "walmart_generator_mode",
    "Current generator mode represented as one-hot gauges.",
    ("mode",),
)
GENERATOR_INTERVAL = Gauge(
    "walmart_generator_interval_seconds",
    "Configured delay between two generated orders.",
)
GENERATOR_LAST_ORDER = Gauge(
    "walmart_generator_last_order_timestamp_seconds",
    "Unix timestamp of the most recently generated order.",
)
GENERATOR_ORDER_ITEMS = Histogram(
    "walmart_generator_order_items",
    "Number of order items produced per generated order.",
    buckets=(1, 2, 3, 4, 5, 6, 8, 10, 15, 20),
)


def set_generator_mode(mode: str) -> None:
    """Publish a bounded, low-cardinality one-hot mode metric."""

    for known_mode in GENERATOR_MODES:
        GENERATOR_MODE.labels(mode=known_mode).set(
            1 if known_mode == mode else 0
        )


def write_prometheus_discovery_file(port: int) -> None:
    """Expose the dynamically selected host port to containerized Prometheus."""

    configured_path = os.getenv("GENERATOR_PROMETHEUS_DISCOVERY_FILE")
    if configured_path:
        discovery_path = Path(configured_path).expanduser().resolve()
    else:
        project_root = Path(__file__).resolve().parents[2]
        discovery_path = (
            project_root
            / "airflow"
            / "monitoring"
            / "runtime"
            / "generator-targets.json"
        )

    target = os.getenv(
        "GENERATOR_PROMETHEUS_TARGET",
        f"host.docker.internal:{port}",
    )
    payload = [
        {
            "targets": [target],
            "labels": {
                "service": "walmart-generator",
                "environment": "local",
            },
        }
    ]

    discovery_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = discovery_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(discovery_path)

FIRST_NAMES = (
    "Alex", "Amélie", "Arthur", "Camille", "Chloé", "Élodie",
    "Emma", "Hugo", "Jade", "Léa", "Liam", "Lucas",
    "Maya", "Nathan", "Nora", "Olivia", "Raphaël", "Sofia",
)
LAST_NAMES = (
    "Bélanger", "Bouchard", "Caron", "Dubois", "Fortin", "Gagnon",
    "Girard", "Lavoie", "Martin", "Morin", "Nguyen", "Roy",
    "Simard", "Tremblay", "Wong",
)
LOCATIONS = (
    ("Montréal", "Québec", "Canada", "514"),
    ("Québec", "Québec", "Canada", "418"),
    ("Toronto", "Ontario", "Canada", "416"),
    ("Ottawa", "Ontario", "Canada", "613"),
    ("Vancouver", "Colombie-Britannique", "Canada", "604"),
    ("Calgary", "Alberta", "Canada", "403"),
    ("Edmonton", "Alberta", "Canada", "780"),
    ("Halifax", "Nouvelle-Écosse", "Canada", "902"),
    ("Winnipeg", "Manitoba", "Canada", "204"),
)
EMAIL_DOMAINS = ("mail.ca", "inbox.ca", "clientmail.ca")

app = Flask(__name__)


def utc_now_without_timezone() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def order_to_dict(order: GeneratedOrder) -> dict[str, Any]:
    return {
        "event_type": "order",
        "order_id": order.order_id,
        "customer_id": order.customer.customer_id,
        "customer_name": order.customer.display_name,
        "store_id": order.store_employee.store_id,
        "store_name": order.store_employee.store_name,
        "employee_id": order.store_employee.employee_id,
        "employee_name": order.store_employee.employee_name,
        "order_item_count": order.order_item_count,
        "total_amount": str(order.total_amount),
        "generated_at": utc_iso(),
    }


def email_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return (
        normalized.encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .replace(" ", "-")
        .replace("'", "")
    )


def create_customer_signup() -> dict[str, Any]:
    """Generate and persist one synthetic, active Canadian customer."""

    connection = connect(get_connection_string())
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (CUSTOMER_ADVISORY_LOCK_ID,),
            )
            cursor.execute(
                "SELECT COALESCE(MAX(customer_id), 0) + 1 FROM raw.customers"
            )
            customer_id = int(cursor.fetchone()[0])

            first_name = RANDOM.choice(FIRST_NAMES)
            last_name = RANDOM.choice(LAST_NAMES)
            city, province, country, area_code = RANDOM.choice(LOCATIONS)
            domain = RANDOM.choice(EMAIL_DOMAINS)
            email = (
                f"{email_slug(first_name)}.{email_slug(last_name)}."
                f"{customer_id}@{domain}"
            )
            phone = (
                f"+1 {area_code}-"
                f"{RANDOM.randint(200, 999):03d}-"
                f"{RANDOM.randint(1000, 9999):04d}"
            )
            created_at = utc_now_without_timezone()

            cursor.execute(
                """
                INSERT INTO raw.customers (
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone,
                    city,
                    province,
                    country,
                    created_timestamp,
                    updated_timestamp,
                    is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Y')
                """,
                (
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone,
                    city,
                    province,
                    country,
                    created_at,
                    created_at,
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "event_type": "customer_signup",
        "customer_id": customer_id,
        "customer_name": f"{first_name} {last_name}",
        "email": email,
        "phone": phone,
        "city": city,
        "province": province,
        "country": country,
        "generated_at": utc_iso(),
    }


class GeneratorController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._mode = "idle"
        self._interval = DEFAULT_INTERVAL_SECONDS
        self._active_customer_id: int | None = None
        self._active_customer_name: str | None = None
        self._orders_generated = 0
        self._customers_signed_up = 0
        self._last_signup: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_order: dict[str, Any] | None = None
        self._activity: deque[dict[str, Any]] = deque(maxlen=MAX_ACTIVITY_ITEMS)
        GENERATOR_INTERVAL.set(self._interval)
        set_generator_mode(self._mode)

    def start(self, interval: float) -> tuple[bool, str]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False, "Le générateur est déjà actif."

            self._stop_event = threading.Event()
            self._mode = "starting"
            self._interval = interval
            GENERATOR_INTERVAL.set(interval)
            set_generator_mode(self._mode)
            self._active_customer_id = None
            self._active_customer_name = None
            self._orders_generated = 0
            self._last_error = None
            self._last_order = None
            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="ghost-order-generator",
            )
            self._thread.start()

        return True, "Connexion à Ghost en cours."

    def stop(self) -> tuple[bool, str]:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._mode = "idle"
                set_generator_mode(self._mode)
                return False, "Le générateur est déjà arrêté."
            self._mode = "stopping"
            set_generator_mode(self._mode)
            self._stop_event.set()
        return True, "Arrêt demandé."

    def add_customer_signup(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._customers_signed_up += 1
            SIGNUP_EVENTS.inc()
            self._last_signup = event
            self._activity.appendleft(event)

    def _next_random_customer(self, connection: Any, previous_id: int | None) -> Any:
        customer = select_customer(connection, None)
        for _ in range(5):
            if customer.customer_id != previous_id:
                break
            customer = select_customer(connection, None)
        return customer

    def _run(self) -> None:
        connection = None
        previous_store_id: int | None = None
        previous_customer_id: int | None = None

        try:
            connection = connect(get_connection_string())
            with self._lock:
                self._mode = "running"
                set_generator_mode(self._mode)

            while not self._stop_event.is_set():
                customer = self._next_random_customer(
                    connection,
                    previous_customer_id,
                )
                order = create_order(
                    connection,
                    customer=customer,
                    previous_store_id=previous_store_id,
                )
                previous_customer_id = customer.customer_id
                previous_store_id = order.store_employee.store_id
                serialized = order_to_dict(order)

                with self._lock:
                    self._active_customer_id = customer.customer_id
                    self._active_customer_name = customer.display_name
                    self._orders_generated += 1
                    ORDER_EVENTS.inc()
                    GENERATOR_ORDER_ITEMS.observe(order.order_item_count)
                    GENERATOR_LAST_ORDER.set(datetime.now(timezone.utc).timestamp())
                    self._last_order = serialized
                    self._activity.appendleft(serialized)
                    self._last_error = None

                if self._stop_event.wait(self._interval):
                    break
        except Exception as exc:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
            with self._lock:
                self._last_error = str(exc)
                self._mode = "error"
                GENERATOR_ERRORS.labels(event_type="order").inc()
                set_generator_mode(self._mode)
        finally:
            if connection is not None:
                connection.close()
            with self._lock:
                if self._mode != "error":
                    self._mode = "idle"
                    set_generator_mode(self._mode)

    def status(self) -> dict[str, Any]:
        with self._lock:
            seconds_until_next = None
            if self._mode == "running" and self._last_order:
                generated_at = datetime.fromisoformat(
                    self._last_order["generated_at"]
                )
                elapsed = (
                    datetime.now(timezone.utc) - generated_at
                ).total_seconds()
                seconds_until_next = max(0.0, self._interval - elapsed)

            return {
                "mode": self._mode,
                "interval_seconds": self._interval,
                "active_customer_id": self._active_customer_id,
                "active_customer_name": self._active_customer_name,
                "orders_generated": self._orders_generated,
                "customers_signed_up": self._customers_signed_up,
                "last_signup": self._last_signup,
                "last_error": self._last_error,
                "last_order": self._last_order,
                "activity": list(self._activity),
                "seconds_until_next": seconds_until_next,
            }


controller = GeneratorController()


@app.get("/")
def index() -> str:
    return render_template("generator_index.html")


@app.get("/api/status")
def api_status() -> Any:
    return jsonify(controller.status())


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus endpoint containing technical, low-cardinality metrics."""

    return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)


@app.post("/api/start")
def api_start() -> Any:
    payload = request.get_json(silent=True) or {}

    try:
        interval = float(payload.get("interval_seconds", DEFAULT_INTERVAL_SECONDS))
    except (TypeError, ValueError):
        return jsonify(
            {"ok": False, "message": "L’intervalle doit être un nombre."}
        ), 400

    if not MIN_INTERVAL_SECONDS <= interval <= MAX_INTERVAL_SECONDS:
        return jsonify(
            {
                "ok": False,
                "message": (
                    f"L’intervalle doit être compris entre "
                    f"{MIN_INTERVAL_SECONDS:g} et {MAX_INTERVAL_SECONDS:g} secondes."
                ),
            }
        ), 400

    started, message = controller.start(interval=interval)
    return jsonify({"ok": started, "message": message}), 200 if started else 409


@app.post("/api/stop")
def api_stop() -> Any:
    stopped, message = controller.stop()
    return jsonify({"ok": stopped, "message": message})


@app.post("/api/customers/signup")
def api_customer_signup() -> Any:
    try:
        event = create_customer_signup()
    except Exception as exc:
        GENERATOR_ERRORS.labels(event_type="customer_signup").inc()
        return jsonify(
            {
                "ok": False,
                "message": f"Sign up impossible dans PostgreSQL : {exc}",
            }
        ), 500

    controller.add_customer_signup(event)
    return jsonify(
        {
            "ok": True,
            "message": (
                f"Client #{event['customer_id']} généré et inscrit. "
                "Il est maintenant éligible aux prochaines commandes."
            ),
            "customer": event,
        }
    ), 201


def find_available_port(preferred: int = DEFAULT_PORT) -> int:
    for port in range(preferred, preferred + 50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("Aucun port local libre trouvé.")


if __name__ == "__main__":
    selected_port = find_available_port()
    write_prometheus_discovery_file(selected_port)
    bind_host = os.getenv("GENERATOR_BIND_HOST", "127.0.0.1")
    print(f"Order Pulse: http://127.0.0.1:{selected_port}")
    app.run(
        host=bind_host,
        port=selected_port,
        debug=False,
        threaded=True,
        use_reloader=False,
    )