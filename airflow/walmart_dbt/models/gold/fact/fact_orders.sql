-- Star-schema fact table. Grain: one row per order line (order_item_id).
-- Only foreign keys to the dimensions, the degenerate order identifiers and
-- additive measures. Descriptive attributes live in the dimensions:
-- channel / payment / status in dim_order_profile, calendar in dim_date,
-- hour of day in dim_time (store local time).
SELECT
    lines.order_item_id,
    lines.order_id,
    CAST(date_format(lines.order_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    HOUR(lines.order_timestamp) AS time_key,
    lines.customer_id,
    lines.product_id,
    lines.store_id,
    lines.employee_id,
    profile.order_profile_key,
    lines.order_timestamp,
    lines.quantity,
    lines.unit_price,
    lines.line_amount AS sales_amount,
    CASE
        WHEN profile.is_net_sale
         AND lines.order_is_active = 'Y'
         AND lines.order_item_is_active = 'Y'
        THEN lines.line_amount
        ELSE 0
    END AS net_sales_amount,
    CASE WHEN lines.order_status = 'Returned' THEN lines.line_amount ELSE 0 END AS returned_amount,
    lines.obt_b_processed_at AS processed_at
FROM {{ ref('obt_b') }} AS lines
LEFT JOIN {{ ref('dim_order_profile') }} AS profile
    ON profile.payment_method = lines.payment_method
   AND profile.order_status = lines.order_status
