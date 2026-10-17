-- Store dimension: current version of each supercentre.
SELECT
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    CASE store_province
        WHEN 'Colombie-Britannique' THEN 'America/Vancouver'
        WHEN 'Alberta' THEN 'America/Edmonton'
        WHEN 'Saskatchewan' THEN 'America/Regina'
        WHEN 'Manitoba' THEN 'America/Winnipeg'
        WHEN 'Nouvelle-Écosse' THEN 'America/Halifax'
        WHEN 'Nouveau-Brunswick' THEN 'America/Moncton'
        WHEN 'Terre-Neuve-et-Labrador' THEN 'America/St_Johns'
        ELSE 'America/Toronto'
    END AS store_timezone,
    '07:00-23:00' AS opening_hours,
    CAST(store_created_timestamp AS DATE) AS store_opened_on,
    store_is_active,
    dbt_scd_id AS store_version_id,
    dbt_valid_from AS version_valid_from
FROM {{ ref('stores_snapshot') }}
WHERE dbt_valid_to = to_date('9999-12-31')
