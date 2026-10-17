-- Supercentres are open from 07:00 to 23:00 (store local time): an in-store
-- sale outside these hours breaks the business rules of the generator.
{{ config(severity='warn') }}

SELECT order_id, order_timestamp, payment_method
FROM {{ ref('orders_t') }}
WHERE payment_method <> 'Online'
  AND (HOUR(order_timestamp) < 7 OR HOUR(order_timestamp) >= 23)
