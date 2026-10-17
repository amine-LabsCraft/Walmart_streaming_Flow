-- Customer dimension: current version of each customer (one row per
-- customer_id). The full SCD2 history stays in snapshots.customers_snapshot.
SELECT
    customer_id,
    customer_first_name,
    customer_last_name,
    CONCAT(customer_first_name, ' ', customer_last_name) AS customer_full_name,
    customer_email,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    CAST(customer_created_timestamp AS DATE) AS customer_since,
    customer_is_active,
    dbt_scd_id AS customer_version_id,
    dbt_valid_from AS version_valid_from
FROM {{ ref('customers_snapshot') }}
WHERE dbt_valid_to = to_date('9999-12-31')
