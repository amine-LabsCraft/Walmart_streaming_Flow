"""Local web interface for the Ghost PostgreSQL order generator."""

from __future__ import annotations

import socket
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, render_template, request

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
MAX_ACTIVITY_ITEMS = 30
DEFAULT_PORT = 5050

app = Flask(__name__)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def order_to_dict(order: GeneratedOrder) -> dict[str, Any]:
    return {
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


class GeneratorController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._mode = "idle"
        self._interval = DEFAULT_INTERVAL_SECONDS
        self._customer_mode = "random"
        self._fixed_customer_id: int | None = None
        self._active_customer_id: int | None = None
        self._active_customer_name: str | None = None
        self._orders_generated = 0
        self._last_error: str | None = None
        self._last_order: dict[str, Any] | None = None
        self._activity: deque[dict[str, Any]] = deque(maxlen=MAX_ACTIVITY_ITEMS)

    def start(
        self,
        interval: float,
        customer_mode: str,
        fixed_customer_id: int | None,
    ) -> tuple[bool, str]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False, "Le générateur est déjà actif."

            self._stop_event = threading.Event()
            self._mode = "starting"
            self._interval = interval
            self._customer_mode = customer_mode
            self._fixed_customer_id = fixed_customer_id
            self._active_customer_id = None
            self._active_customer_name = None
            self._orders_generated = 0
            self._last_error = None
            self._last_order = None
            self._activity.clear()
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
                return False, "Le générateur est déjà arrêté."
            self._mode = "stopping"
            self._stop_event.set()
        return True, "Arrêt demandé."

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
            fixed_customer = None
            if self._customer_mode == "fixed":
                fixed_customer = select_customer(
                    connection,
                    self._fixed_customer_id,
                )

            with self._lock:
                self._mode = "running"

            while not self._stop_event.is_set():
                if fixed_customer is None:
                    customer = self._next_random_customer(
                        connection,
                        previous_customer_id,
                    )
                else:
                    customer = fixed_customer

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
        finally:
            if connection is not None:
                connection.close()
            with self._lock:
                if self._mode != "error":
                    self._mode = "idle"

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
                "customer_mode": self._customer_mode,
                "fixed_customer_id": self._fixed_customer_id,
                "active_customer_id": self._active_customer_id,
                "active_customer_name": self._active_customer_name,
                "orders_generated": self._orders_generated,
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


@app.post("/api/start")
def api_start() -> Any:
    payload = request.get_json(silent=True) or {}
    customer_mode = str(payload.get("customer_mode", "random"))

    if customer_mode not in {"random", "fixed"}:
        return jsonify(
            {"ok": False, "message": "Mode client invalide."}
        ), 400

    try:
        interval = float(payload.get("interval_seconds", DEFAULT_INTERVAL_SECONDS))
    except (TypeError, ValueError):
        return jsonify(
            {"ok": False, "message": "L'intervalle doit être un nombre."}
        ), 400

    if not MIN_INTERVAL_SECONDS <= interval <= MAX_INTERVAL_SECONDS:
        return jsonify(
            {
                "ok": False,
                "message": (
                    f"L'intervalle doit être compris entre "
                    f"{MIN_INTERVAL_SECONDS:g} et {MAX_INTERVAL_SECONDS:g} secondes."
                ),
            }
        ), 400

    fixed_customer_id = None
    if customer_mode == "fixed":
        try:
            fixed_customer_id = int(payload.get("customer_id"))
        except (TypeError, ValueError):
            return jsonify(
                {
                    "ok": False,
                    "message": "Indiquez un customer_id entier pour le mode fixe.",
                }
            ), 400
        if fixed_customer_id <= 0:
            return jsonify(
                {
                    "ok": False,
                    "message": "Le customer_id doit être supérieur à zéro.",
                }
            ), 400

    started, message = controller.start(
        interval=interval,
        customer_mode=customer_mode,
        fixed_customer_id=fixed_customer_id,
    )
    return jsonify({"ok": started, "message": message}), 200 if started else 409


@app.post("/api/stop")
def api_stop() -> Any:
    stopped, message = controller.stop()
    return jsonify({"ok": stopped, "message": message})


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
    print(f"Order Pulse: http://127.0.0.1:{selected_port}")
    app.run(
        host="127.0.0.1",
        port=selected_port,
        debug=False,
        threaded=True,
        use_reloader=False,
    )
