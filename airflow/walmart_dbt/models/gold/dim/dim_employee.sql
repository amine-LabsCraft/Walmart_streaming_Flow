-- Employee dimension: current version of each employee and role.
-- store_id is a descriptive attribute here (the employee's store); the fact
-- table relates to stores through its own store_id key.
SELECT
    employee_id,
    employee_first_name,
    employee_last_name,
    CONCAT(employee_first_name, ' ', employee_last_name) AS employee_full_name,
    employee_email,
    job_title,
    CASE
        WHEN job_title IN ('Caissier', 'Caissière', 'Superviseur des caisses') THEN 'Caisses'
        WHEN job_title = 'Préparateur de commandes en ligne' THEN 'Commandes en ligne'
        WHEN job_title IN ('Directeur de magasin', 'Directeur adjoint') THEN 'Direction'
        ELSE 'Rayons et service'
    END AS role_group,
    salary,
    store_id AS employee_store_id,
    CAST(employee_created_timestamp AS DATE) AS hired_on,
    employee_is_active,
    dbt_scd_id AS employee_version_id,
    dbt_valid_from AS version_valid_from
FROM {{ ref('employees_snapshot') }}
WHERE dbt_valid_to = to_date('9999-12-31')
