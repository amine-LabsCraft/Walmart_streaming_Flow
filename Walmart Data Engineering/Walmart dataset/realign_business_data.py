"""Realign the existing source data on supermarket business rules, in place.

The seed CSV files were produced by a random data faker: US states in Canada,
uniform prices, future order dates, 25 % cancelled orders... This script
rewrites the *values* of the existing rows so that history follows the same
business rules as the real-time generator (``business_rules.py``):

* stores in real Canadian cities, a supercentre catalogue, store roles;
* customers living near a home store, with heavy-tailed loyalty;
* one year of orders with seasonality, weekdays, opening hours (local time),
  realistic baskets, payment mix and status lifecycle.

Nothing is deleted and every identifier is kept: rows are only UPDATEd, so
the PostgreSQL triggers bump ``change_version`` and the next Airflow run
propagates the corrections to Databricks (bronze MERGE, Silver MERGE, new
SCD2 versions in Gold) without rebuilding the lakehouse.

    python realign_business_data.py            # dry run with a summary
    python realign_business_data.py --apply    # write the changes
"""

from __future__ import annotations

import argparse
import bisect
import random
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import mean

import psycopg2
from psycopg2.extras import execute_values

import business_rules as rules
from continuous_order_generator import get_connection_string


SEED = 20261016
WINDOW_DAYS = 365
INACTIVE_CUSTOMER_QUIET_DAYS = 90   # closed accounts stopped shopping
DISCONTINUED_PRODUCT_QUIET_DAYS = 180


def slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return (
        normalized.encode("ascii", "ignore").decode("ascii")
        .lower().replace(" ", "").replace("'", "").replace("-", "")
    )


def random_moment(rng: random.Random, start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=rng.uniform(0, (end - start).total_seconds()))


class WeightedPicker:
    """Weighted sampling restricted to the first ``limit`` items."""

    def __init__(self, items: list, weights: list[float]) -> None:
        self.items = items
        self.cumulative: list[float] = []
        total = 0.0
        for weight in weights:
            total += weight
            self.cumulative.append(total)

    def pick(self, rng: random.Random, limit: int | None = None):
        limit = len(self.items) if limit is None else limit
        if limit <= 0:
            raise ValueError("empty population")
        threshold = rng.uniform(0, self.cumulative[limit - 1])
        return self.items[bisect.bisect_left(self.cumulative, threshold, 0, limit - 1)]


def load(cursor) -> dict:
    data = {}
    cursor.execute("SELECT store_id FROM raw.stores ORDER BY store_id")
    data["stores"] = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        "SELECT employee_id, store_id, first_name, last_name, is_active = 'Y' "
        "FROM raw.employees ORDER BY employee_id"
    )
    data["employees"] = cursor.fetchall()
    cursor.execute(
        "SELECT customer_id, first_name, last_name, email, city, province, "
        "created_timestamp, is_active = 'Y' FROM raw.customers ORDER BY customer_id"
    )
    data["customers"] = cursor.fetchall()
    cursor.execute("SELECT product_id, is_active = 'Y' FROM raw.products ORDER BY product_id")
    data["products"] = cursor.fetchall()
    cursor.execute("SELECT order_id FROM raw.orders ORDER BY order_id")
    data["orders"] = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT order_id, order_item_id FROM raw.order_items ORDER BY order_item_id")
    items_by_order = defaultdict(list)
    for order_id, order_item_id in cursor.fetchall():
        items_by_order[order_id].append(order_item_id)
    data["items_by_order"] = items_by_order
    return data


def build(data: dict, now_utc: datetime) -> dict:
    rng = random.Random(SEED)
    stores = rules.STORES
    if sorted(stores) != data["stores"]:
        raise RuntimeError("Expected store_id 1..25 in raw.stores")

    # ------------------------------------------------------------------ #
    # Catalogue
    # ------------------------------------------------------------------ #
    catalog = rules.build_catalog()
    if len(catalog) != len(data["products"]):
        raise RuntimeError("Catalogue size does not match raw.products")
    products = {}
    for (product_id, is_active), item in zip(data["products"], catalog):
        products[product_id] = {"item": item, "active": is_active}

    by_category = defaultdict(list)
    for product_id, product in products.items():
        by_category[product["item"].category].append(product_id)
    category_pickers = {
        category: WeightedPicker(
            ids, [rules.product_weight(pid, products[pid]["item"].popularity) for pid in ids]
        )
        for category, ids in by_category.items()
    }

    # ------------------------------------------------------------------ #
    # Employees: roles per store
    # ------------------------------------------------------------------ #
    employees = {}
    store_staff = defaultdict(list)
    for employee_id, store_id, first, last, is_active in data["employees"]:
        employees[employee_id] = {"store": store_id, "first": first, "last": last, "active": is_active}
        store_staff[store_id].append((employee_id, is_active))
    for store_id, staff in store_staff.items():
        for employee_id, title in rules.assign_store_roles(staff, rng).items():
            employees[employee_id]["title"] = title
            employees[employee_id]["salary"] = rules.salary_for(title, rng)
    cashiers = defaultdict(list)
    pickers = defaultdict(list)
    for employee_id, employee in employees.items():
        if not employee["active"]:
            continue
        if employee["title"] in rules.CASHIER_TITLES:
            cashiers[employee["store"]].append(employee_id)
        elif employee["title"] == rules.PICKER_TITLE:
            pickers[employee["store"]].append(employee_id)
    for store_id in stores:
        if not cashiers[store_id] or not pickers[store_id]:
            raise RuntimeError(f"Store {store_id} lacks an active cashier or picker")
    seen_emails = set()
    for employee_id, employee in employees.items():
        email = f"{slug(employee['first'])}.{slug(employee['last'])}@walmart.ca"
        if email in seen_emails:
            email = f"{slug(employee['first'])}.{slug(employee['last'])}{employee_id}@walmart.ca"
        seen_emails.add(email)
        employee["email"] = email

    # ------------------------------------------------------------------ #
    # Customers: home store, location, creation date, loyalty
    # ------------------------------------------------------------------ #
    store_ids = list(stores)
    store_picker = WeightedPicker(store_ids, [stores[s].traffic_weight for s in store_ids])
    stores_by_province = defaultdict(list)
    for store in stores.values():
        stores_by_province[store.province].append(store.store_id)

    seed_customers = [row for row in data["customers"] if row[0] <= 2000]
    first_created = datetime(2022, 3, 1)
    last_created = datetime(2026, 9, 30)
    created_dates = sorted(random_moment(rng, first_created, last_created) for _ in seed_customers)

    customers = {}
    for index, (customer_id, first, last, email, city, province, created, is_active) in enumerate(data["customers"]):
        if customer_id <= 2000:
            home = store_picker.pick(rng)
            created = created_dates[index]
        else:
            # Customers signed up through Order Pulse keep their identity;
            # their home store is the closest store we can infer.
            same_city = [s for s in store_ids if stores[s].city == city]
            same_province = stores_by_province.get(province, [])
            home = (same_city or same_province or [store_picker.pick(rng)])[0]
        store = stores[home]
        phone = f"+1 {store.area_code}-{rng.randint(200, 999)}-{rng.randint(1000, 9999)}"
        if customer_id <= 2000:
            domain = rng.choice(rules.email_domains(store.province))
            email = f"{slug(first)}.{slug(last)}{customer_id}@{domain}"
        customers[customer_id] = {
            "first": first, "last": last, "email": email, "home": home,
            "city": store.city, "province": store.province, "phone": phone,
            "created": created, "active": is_active,
        }

    by_created = sorted(customers, key=lambda cid: customers[cid]["created"])
    created_keys = [customers[cid]["created"] for cid in by_created]
    customer_picker = WeightedPicker(by_created, [rules.loyalty_weight(cid) for cid in by_created])

    # ------------------------------------------------------------------ #
    # Order timeline: seasonality, weekdays, opening hours
    # ------------------------------------------------------------------ #
    # Latest allowed local time: "now" in the westernmost store time zone,
    # so that no store-local timestamp lies in the future.
    vancouver = rules.local_now(stores[15], now_utc.replace(tzinfo=timezone.utc)).replace(tzinfo=None)
    window_end = vancouver - timedelta(minutes=10)
    window_start = (window_end - timedelta(days=WINDOW_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    days = []
    day = window_start.date()
    while day <= window_end.date():
        days.append(day)
        day += timedelta(days=1)
    day_picker = WeightedPicker(days, [rules.day_factor(d) for d in days])

    slots, slot_weights = [], []
    in_store_total = sum(rules.IN_STORE_HOURLY.values())
    online_total = sum(rules.ONLINE_HOURLY.values())
    for hour, weight in rules.IN_STORE_HOURLY.items():
        slots.append(("In-store", hour))
        slot_weights.append((1 - rules.ONLINE_SHARE) * weight / in_store_total)
    for hour, weight in rules.ONLINE_HOURLY.items():
        slots.append(("Online", hour))
        slot_weights.append(rules.ONLINE_SHARE * weight / online_total)
    slot_picker = WeightedPicker(slots, slot_weights)

    events = []
    while len(events) < len(data["orders"]):
        day = day_picker.pick(rng)
        channel, hour = slot_picker.pick(rng)
        moment = datetime(day.year, day.month, day.day, hour) + timedelta(seconds=rng.randint(0, 3599))
        if window_start <= moment <= window_end:
            events.append((moment, channel))
    events.sort()

    # ------------------------------------------------------------------ #
    # Orders and lines
    # ------------------------------------------------------------------ #
    orders, lines = {}, {}
    product_first_sale = {}
    employee_first_order = {}
    for order_id, (order_ts, channel) in zip(data["orders"], events):
        # Customer: only people already registered, closed accounts stop shopping.
        limit = bisect.bisect_right(created_keys, order_ts - timedelta(hours=6))
        for _ in range(30):
            customer_id = customer_picker.pick(rng, max(limit, 1))
            customer = customers[customer_id]
            if customer["active"] or (window_end - order_ts).days > INACTIVE_CUSTOMER_QUIET_DAYS:
                break

        home = customer["home"]
        draw = rng.random()
        if channel == "Online":
            store_id = home if draw < 0.95 else rng.choice(stores_by_province[stores[home].province])
        elif draw < rules.HOME_STORE_SHARE:
            store_id = home
        elif draw < rules.HOME_STORE_SHARE + rules.SAME_PROVINCE_SHARE:
            store_id = rng.choice(stores_by_province[stores[home].province])
        else:
            store_id = store_picker.pick(rng)
        store = stores[store_id]

        if channel == "Online":
            employee_id = rng.choice(pickers[store_id])
            payment = rules.ONLINE_PAYMENT
        else:
            employee_id = rng.choice(cashiers[store_id])
            payment = rules.weighted_choice(rng, rules.IN_STORE_PAYMENTS)

        old_enough = (window_end - order_ts).days >= 1
        draw = rng.random()
        if channel == "Online":
            if draw < rules.ONLINE_CANCEL_RATE:
                status = "Cancelled"
            elif draw < rules.ONLINE_CANCEL_RATE + rules.ONLINE_RETURN_RATE and old_enough:
                status = "Returned"
            else:
                status = "Completed"
        else:
            status = "Returned" if draw < rules.IN_STORE_RETURN_RATE and old_enough else "Completed"

        created_utc = rules.to_utc_naive(order_ts, store)
        recent = (window_end - order_ts).days <= DISCONTINUED_PRODUCT_QUIET_DAYS
        chosen = set()
        total = Decimal("0.00")
        for order_item_id in data["items_by_order"].get(order_id, []):
            for _ in range(50):
                category = rules.weighted_choice(rng, rules.CATEGORY_LINE_WEIGHTS)
                product_id = category_pickers[category].pick(rng)
                if product_id in chosen:
                    continue
                if recent and not products[product_id]["active"]:
                    continue
                break
            chosen.add(product_id)
            item = products[product_id]["item"]
            quantity = rules.weighted_choice(rng, rules.QUANTITY_WEIGHTS[item.category])
            line_amount = rules.money(item.price * quantity)
            total += line_amount
            lines[order_item_id] = (product_id, quantity, item.price, line_amount, created_utc)
            product_first_sale[product_id] = min(product_first_sale.get(product_id, created_utc), created_utc)

        orders[order_id] = {
            "customer": customer_id, "store": store_id, "employee": employee_id,
            "order_ts": order_ts, "payment": payment, "status": status,
            "total": rules.money(total), "created": created_utc,
        }
        employee_first_order[employee_id] = min(employee_first_order.get(employee_id, created_utc), created_utc)

    # ------------------------------------------------------------------ #
    # Chronology of reference data
    # ------------------------------------------------------------------ #
    for employee_id, employee in employees.items():
        first = employee_first_order.get(employee_id)
        if first:
            employee["created"] = first - timedelta(days=rng.randint(20, 900), seconds=rng.randint(0, 86399))
        else:
            employee["created"] = random_moment(rng, datetime(2019, 1, 1), datetime(2026, 6, 1))
    store_opened = {}
    for store_id in stores:
        staff_hired = [employees[e]["created"] for e, _ in store_staff[store_id]]
        store_opened[store_id] = min(staff_hired) - timedelta(days=rng.randint(30, 400))
    for product_id, product in products.items():
        first = product_first_sale.get(product_id)
        if first:
            product["created"] = first - timedelta(days=rng.randint(30, 700), seconds=rng.randint(0, 86399))
        else:
            product["created"] = random_moment(rng, datetime(2020, 1, 1), datetime(2026, 6, 1))

    return {
        "now": now_utc, "stores": stores, "store_opened": store_opened,
        "products": products, "employees": employees, "customers": customers,
        "orders": orders, "lines": lines, "window": (window_start, window_end),
    }


def summarize(plan: dict) -> None:
    orders = plan["orders"].values()
    statuses = Counter(order["status"] for order in orders)
    payments = Counter(order["payment"] for order in orders)
    completed = [float(order["total"]) for order in orders if order["status"] == "Completed"]
    home = sum(1 for order in orders if order["store"] == plan["customers"][order["customer"]]["home"])
    hours = Counter(order["order_ts"].hour for order in orders if order["payment"] != "Online")
    months = Counter(order["order_ts"].strftime("%Y-%m") for order in orders)
    loyalty = Counter(order["customer"] for order in orders)
    top = sum(count for _, count in loyalty.most_common(len(plan["customers"]) // 5))
    categories = Counter(
        plan["products"][line[0]]["item"].category for line in plan["lines"].values()
    )
    print(f"Window             : {plan['window'][0]} -> {plan['window'][1]}")
    print(f"Orders             : {len(plan['orders'])}  lines: {len(plan['lines'])}")
    print(f"Status mix         : {dict(statuses)}")
    print(f"Payment mix        : {dict(payments)}")
    print(f"Average basket     : {mean(completed):.2f} $ (completed orders)")
    print(f"At home store      : {100 * home / len(plan['orders']):.1f} %")
    print(f"Top 20 % customers : {100 * top / len(plan['orders']):.1f} % of orders")
    print(f"Lines by category  : {dict(categories)}")
    print(f"In-store hours     : {min(hours)}h-{max(hours)}h local, peak {hours.most_common(1)[0][0]}h")
    print(f"Orders per month   : {dict(sorted(months.items()))}")


def apply(connection, plan: dict) -> None:
    now = plan["now"]
    with connection.cursor() as cursor:
        execute_values(cursor, """
            UPDATE raw.stores AS t SET store_name = v.name, city = v.city, province = v.province,
                country = 'Canada', created_timestamp = v.created, updated_timestamp = v.updated
            FROM (VALUES %s) AS v(store_id, name, city, province, created, updated)
            WHERE t.store_id = v.store_id
        """, [(s.store_id, s.name, s.city, s.province, plan["store_opened"][s.store_id], now)
              for s in plan["stores"].values()])

        execute_values(cursor, """
            UPDATE raw.products AS t SET product_name = v.name, category = v.category, brand = v.brand,
                price = v.price, created_timestamp = v.created, updated_timestamp = v.updated
            FROM (VALUES %s) AS v(product_id, name, category, brand, price, created, updated)
            WHERE t.product_id = v.product_id
        """, [(pid, p["item"].name, p["item"].category, p["item"].brand, p["item"].price, p["created"], now)
              for pid, p in plan["products"].items()], page_size=1000)

        execute_values(cursor, """
            UPDATE raw.employees AS t SET job_title = v.title, salary = v.salary, email = v.email,
                created_timestamp = v.created, updated_timestamp = v.updated
            FROM (VALUES %s) AS v(employee_id, title, salary, email, created, updated)
            WHERE t.employee_id = v.employee_id
        """, [(eid, e["title"], e["salary"], e["email"], e["created"], now)
              for eid, e in plan["employees"].items()], page_size=1000)

        execute_values(cursor, """
            UPDATE raw.customers AS t SET email = v.email, phone = v.phone, city = v.city,
                province = v.province, country = 'Canada',
                created_timestamp = v.created, updated_timestamp = v.updated
            FROM (VALUES %s) AS v(customer_id, email, phone, city, province, created, updated)
            WHERE t.customer_id = v.customer_id
        """, [(cid, c["email"], c["phone"], c["city"], c["province"], c["created"], now)
              for cid, c in plan["customers"].items()], page_size=1000)

        execute_values(cursor, """
            UPDATE raw.orders AS t SET customer_id = v.customer_id, store_id = v.store_id,
                employee_id = v.employee_id, order_timestamp = v.order_ts,
                payment_method = v.payment, order_status = v.status, total_amount = v.total,
                created_timestamp = v.created, updated_timestamp = v.updated, is_active = 'Y'
            FROM (VALUES %s) AS v(order_id, customer_id, store_id, employee_id, order_ts,
                                  payment, status, total, created, updated)
            WHERE t.order_id = v.order_id
        """, [(oid, o["customer"], o["store"], o["employee"], o["order_ts"], o["payment"],
               o["status"], o["total"], o["created"], now)
              for oid, o in plan["orders"].items()], page_size=2000)

        execute_values(cursor, """
            UPDATE raw.order_items AS t SET product_id = v.product_id, quantity = v.quantity,
                unit_price = v.unit_price, line_amount = v.line_amount,
                created_timestamp = v.created, updated_timestamp = v.updated, is_active = 'Y'
            FROM (VALUES %s) AS v(order_item_id, product_id, quantity, unit_price, line_amount,
                                  created, updated)
            WHERE t.order_item_id = v.order_item_id
        """, [(iid, l[0], l[1], l[2], l[3], l[4], now) for iid, l in plan["lines"].items()],
            page_size=2000)
    connection.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="write the changes")
    parser.add_argument("--force", action="store_true", help="realign again an aligned database")
    arguments = parser.parse_args()

    connection = psycopg2.connect(get_connection_string(), connect_timeout=15)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM raw.stores WHERE store_name LIKE 'Walmart Supercentre%%'")
            if cursor.fetchone()[0] and not arguments.force:
                raise SystemExit("Source already realigned. Use --force to rewrite it again.")
            data = load(cursor)
        connection.rollback()

        plan = build(data, datetime.now(timezone.utc).replace(tzinfo=None))
        summarize(plan)
        if not arguments.apply:
            print("\nDry run only. Pass --apply to update the source database.")
            return
        apply(connection, plan)
        print("\nSource database realigned. Run the Airflow DAG to propagate the changes.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
