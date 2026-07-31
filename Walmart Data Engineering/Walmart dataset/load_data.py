from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"
CSV_TABLES = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}


def read_environment(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-16").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def main() -> None:
    connection_string = read_environment(ROOT / ".env")["POSTGRES_CONNECTION"]

    with psycopg2.connect(connection_string, connect_timeout=15) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema || '.' || table_name
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                """
            )
            existing_tables = {row[0] for row in cursor.fetchall()}
            missing_tables = set(CSV_TABLES.values()) - existing_tables
            if missing_tables:
                raise RuntimeError(
                    "Missing target tables: " + ", ".join(sorted(missing_tables))
                )

            existing_rows = {}
            for table_name in CSV_TABLES.values():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                if count:
                    existing_rows[table_name] = count
            if existing_rows:
                details = ", ".join(
                    f"{table}={count}" for table, count in existing_rows.items()
                )
                raise RuntimeError(
                    "Load cancelled because target tables are not empty: " + details
                )

            for csv_name, table_name in CSV_TABLES.items():
                csv_path = DATA_DIR / csv_name
                if not csv_path.is_file():
                    raise FileNotFoundError(csv_path)

                print(f"Loading {csv_name} into {table_name}...")
                with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                    cursor.copy_expert(
                        f"COPY {table_name} FROM STDIN "
                        "WITH (FORMAT CSV, HEADER TRUE)",
                        csv_file,
                    )

            print("\nVerified row counts:")
            for table_name in CSV_TABLES.values():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                print(f"{table_name}: {cursor.fetchone()[0]}")

    print("\nAll CSV data committed successfully.")


if __name__ == "__main__":
    main()
