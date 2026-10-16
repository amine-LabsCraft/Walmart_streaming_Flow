-- First-start initialisation of the local Walmart source database.
-- Executed once by the postgres image when its data volume is empty.
-- /seed is the "Walmart dataset" directory mounted read-only.

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS raw;
SET search_path TO raw;

\i /seed/ddl/walmart_schema.sql

\copy raw.customers FROM '/seed/data/customers.csv' WITH (FORMAT csv, HEADER true)
\copy raw.stores FROM '/seed/data/stores.csv' WITH (FORMAT csv, HEADER true)
\copy raw.products FROM '/seed/data/products.csv' WITH (FORMAT csv, HEADER true)
\copy raw.employees FROM '/seed/data/employees.csv' WITH (FORMAT csv, HEADER true)
\copy raw.orders FROM '/seed/data/orders.csv' WITH (FORMAT csv, HEADER true)
\copy raw.order_items FROM '/seed/data/order_items.csv' WITH (FORMAT csv, HEADER true)

\i /seed/ddl/001_streaming_source_upgrade.sql

CREATE INDEX IF NOT EXISTS order_items_order_id_idx
    ON raw.order_items (order_id);
CREATE INDEX IF NOT EXISTS orders_change_version_idx
    ON raw.orders (change_version);
CREATE INDEX IF NOT EXISTS order_items_change_version_idx
    ON raw.order_items (change_version);
