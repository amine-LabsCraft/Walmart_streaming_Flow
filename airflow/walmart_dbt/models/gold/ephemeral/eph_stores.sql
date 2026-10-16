-- One row per store, read from Silver so that stores without orders are
-- still part of the dimension.
SELECT
    store_id,
    store_name,
    city AS store_city,
    province AS store_province,
    country AS store_country,
    created_timestamp AS store_created_timestamp,
    updated_timestamp AS store_updated_timestamp,
    is_active AS store_is_active,
    processed_at AS store_processed_at,
    CURRENT_TIMESTAMP() AS store_gold_processed_at
FROM {{ ref('stores_t') }}
