-- Additive, idempotent source upgrade for the continuous order generator.
-- Existing tables and historical rows are preserved.

BEGIN;

SELECT pg_advisory_xact_lock(2026073002);

CREATE SEQUENCE IF NOT EXISTS raw.walmart_change_version_seq AS BIGINT;

ALTER TABLE raw.orders
    ADD COLUMN IF NOT EXISTS employee_id BIGINT,
    ADD COLUMN IF NOT EXISTS change_version BIGINT;

ALTER TABLE raw.order_items
    ADD COLUMN IF NOT EXISTS change_version BIGINT;

-- Backfill once so the Databricks query-based connector can perform a clean
-- full refresh using a non-null, strictly increasing cursor.
UPDATE raw.orders
SET change_version = nextval('raw.walmart_change_version_seq')
WHERE change_version IS NULL;

UPDATE raw.order_items
SET change_version = nextval('raw.walmart_change_version_seq')
WHERE change_version IS NULL;

CREATE OR REPLACE FUNCTION raw.assign_walmart_change_version()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.change_version := nextval('raw.walmart_change_version_seq');
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS orders_assign_change_version ON raw.orders;
CREATE TRIGGER orders_assign_change_version
BEFORE INSERT OR UPDATE ON raw.orders
FOR EACH ROW
EXECUTE FUNCTION raw.assign_walmart_change_version();

DROP TRIGGER IF EXISTS order_items_assign_change_version ON raw.order_items;
CREATE TRIGGER order_items_assign_change_version
BEFORE INSERT OR UPDATE ON raw.order_items
FOR EACH ROW
EXECUTE FUNCTION raw.assign_walmart_change_version();

ALTER TABLE raw.orders
    ALTER COLUMN change_version SET NOT NULL;

ALTER TABLE raw.order_items
    ALTER COLUMN change_version SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'orders_employee_id_fkey'
          AND conrelid = 'raw.orders'::regclass
    ) THEN
        ALTER TABLE raw.orders
            ADD CONSTRAINT orders_employee_id_fkey
            FOREIGN KEY (employee_id)
            REFERENCES raw.employees(employee_id)
            NOT VALID;
    END IF;
END;
$$;

COMMENT ON COLUMN raw.orders.employee_id IS
    'Employee who handled the order; nullable for historical orders.';
COMMENT ON COLUMN raw.orders.change_version IS
    'Monotonic CDC cursor assigned on every insert or update.';
COMMENT ON COLUMN raw.order_items.change_version IS
    'Monotonic CDC cursor assigned on every insert or update.';

COMMIT;

-- Validation queries (read-only):
-- SELECT COUNT(*) FILTER (WHERE change_version IS NULL) FROM raw.orders;
-- SELECT COUNT(*) FILTER (WHERE change_version IS NULL) FROM raw.order_items;
-- SELECT employee_id, store_id FROM raw.orders WHERE employee_id IS NOT NULL LIMIT 10;
