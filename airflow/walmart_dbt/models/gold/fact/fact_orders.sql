SELECT 
    order_id,
    order_item_id,
    product_id,
    store_id,
    employee_id,
    customer_id,
    CAST(order_timestamp AS DATE) AS order_date,
    CAST(date_format(order_timestamp, 'yyyyMMdd') AS INT) AS date_key,
    order_timestamp,
    payment_method,
    order_status,
    total_amount,
    total_amount AS order_total_amount,
    quantity,
    unit_price,
    line_amount AS sales_amount,
    line_amount,
    order_is_active,
    order_item_is_active,
    obt_b_processed_at AS processed_at
FROM 
    {{ ref('obt_b') }}