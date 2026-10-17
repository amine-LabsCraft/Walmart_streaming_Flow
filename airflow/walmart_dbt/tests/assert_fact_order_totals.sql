-- The lines of an order in the fact table must add up to the order total
-- recorded in the source (orders_t.total_amount).
SELECT fact.order_id, orders.total_amount, fact.lines_amount
FROM (
    SELECT order_id, SUM(sales_amount) AS lines_amount
    FROM {{ ref('fact_orders') }}
    GROUP BY order_id
) AS fact
JOIN {{ ref('orders_t') }} AS orders
    ON orders.order_id = fact.order_id
WHERE ABS(orders.total_amount - fact.lines_amount) > 0.01
