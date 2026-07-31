SELECT o.order_id
FROM {{ ref('orders_t') }} AS o
LEFT JOIN {{ ref('employees_t') }} AS e
    ON o.employee_id = e.employee_id
WHERE o.employee_id IS NOT NULL
  AND (
      e.employee_id IS NULL
      OR e.store_id <> o.store_id
  )
