-- One row per product, read from Silver so that products never sold are
-- still part of the dimension.
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    created_timestamp AS product_created_timestamp,
    updated_timestamp AS product_updated_timestamp,
    is_active AS product_is_active,
    processed_at AS product_processed_at,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
FROM {{ ref('products_t') }}
