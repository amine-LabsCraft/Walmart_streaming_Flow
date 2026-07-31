SELECT order_id
FROM {{ ref('fact_orders') }}
GROUP BY order_id
HAVING ABS(
    MAX(order_total_amount) - SUM(sales_amount)
) > 0.01
