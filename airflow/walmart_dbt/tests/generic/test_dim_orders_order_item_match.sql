{% test dim_orders_order_item_match(model) %}
WITH expected AS (
    SELECT
        order_id,
        MIN(order_item_id) AS expected_order_item_id
    FROM {{ model }}
    WHERE order_id > 10000
    GROUP BY order_id
),

actual AS (
    SELECT
        order_id,
        order_item_id
    FROM {{ ref('dim_orders') }}
    WHERE order_id > 10000
      AND CAST(dbt_valid_to AS DATE) = DATE '9999-12-31'
)

SELECT
    COALESCE(expected.order_id, actual.order_id) AS order_id,
    expected.expected_order_item_id,
    actual.order_item_id
FROM expected
FULL OUTER JOIN actual
    ON expected.order_id = actual.order_id
WHERE expected.order_id IS NULL
   OR actual.order_id IS NULL
   OR NOT (expected.expected_order_item_id <=> actual.order_item_id)
{% endtest %}
