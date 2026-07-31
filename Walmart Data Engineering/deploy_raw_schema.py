from pathlib import Path

import psycopg

root = Path(__file__).resolve().parent
environment = {
    key: value
    for key, value in (
        line.strip().split("=", 1)
        for line in (root / ".env").read_text(encoding="utf-16").splitlines()
        if "=" in line and line.strip() and not line.lstrip().startswith("#")
    )
}
ddl = (root / "Walmart dataset" / "ddl" / "walmart_schema.sql").read_text()

with psycopg.connect(environment["POSTGRES_CONNECTION"], connect_timeout=15) as connection:
    connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
    connection.execute("SET search_path TO raw")
    connection.execute(ddl)
    tables = connection.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        ("raw",),
    ).fetchall()

print("Created and verified tables:")
for schema, table in tables:
    print(f"{schema}.{table}")



