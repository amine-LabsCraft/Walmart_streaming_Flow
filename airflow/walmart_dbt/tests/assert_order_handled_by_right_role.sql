-- In-store sales are rung by a cashier, online orders are prepared by an
-- online order picker. Reported as a warning: roles may evolve over time.
{{ config(severity='warn') }}

SELECT o.order_id, o.payment_method, e.job_title
FROM {{ ref('orders_t') }} AS o
JOIN {{ ref('employees_t') }} AS e
    ON o.employee_id = e.employee_id
WHERE (o.payment_method = 'Online' AND e.job_title <> 'Préparateur de commandes en ligne')
   OR (o.payment_method <> 'Online' AND e.job_title NOT IN ('Caissier', 'Caissière'))
