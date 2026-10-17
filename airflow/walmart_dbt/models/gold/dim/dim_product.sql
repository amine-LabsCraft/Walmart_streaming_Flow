-- Product dimension: current version of each product (current price,
-- category, brand). Price history stays in snapshots.products_snapshot.
SELECT
    product_id,
    product_name,
    category,
    CASE category
        WHEN 'Grocery' THEN 'Épicerie'
        WHEN 'Home' THEN 'Maison et entretien'
        WHEN 'Clothing' THEN 'Vêtements'
        WHEN 'Electronics' THEN 'Électronique'
        WHEN 'Sports' THEN 'Sports et plein air'
        WHEN 'Toys' THEN 'Jouets'
        ELSE category
    END AS category_label,
    brand,
    brand IN (
        'Great Value', 'George', 'Mainstays', 'Equate', 'onn.', 'Athletic Works',
        'Time and Tru', 'Ozark Trail', 'Hometrends', 'Garanimals', 'Swiss Tech',
        'Adventure Force'
    ) AS is_private_label,
    price AS current_price,
    CASE
        WHEN price < 5 THEN '< 5 $'
        WHEN price < 20 THEN '5 à 20 $'
        WHEN price < 100 THEN '20 à 100 $'
        ELSE '100 $ et plus'
    END AS price_band,
    CAST(product_created_timestamp AS DATE) AS listed_since,
    product_is_active,
    dbt_scd_id AS product_version_id,
    dbt_valid_from AS version_valid_from
FROM {{ ref('products_snapshot') }}
WHERE dbt_valid_to = to_date('9999-12-31')
