-- One row per order header, read from Silver: source of the order
-- lifecycle history (orders_snapshot).
SELECT
    order_id,
    customer_id,
    store_id,
    employee_id,
    order_timestamp,
    payment_method,
    order_status,
    total_amount,
    created_timestamp AS order_created_timestamp,
    updated_timestamp AS order_updated_timestamp,
    is_active AS order_is_active,
    processed_at AS order_processed_at
FROM {{ ref('orders_t') }}
