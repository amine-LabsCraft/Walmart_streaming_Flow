WITH order_grain AS (
    SELECT
        order_id,
        order_item_id,
        payment_method,
        order_status,
        order_timestamp,
        order_created_timestamp,
        order_updated_timestamp,
        order_is_active,
        order_processed_at,
        obt_b_processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_item_id
        ) AS order_item_rank
    FROM {{ ref('obt_b') }}
)

SELECT
    order_id,
    order_item_id,
    payment_method,
    order_status,
    order_timestamp,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_b_processed_at,
    CURRENT_TIMESTAMP() AS order_gold_processed_at
FROM order_grain
WHERE order_item_rank = 1
