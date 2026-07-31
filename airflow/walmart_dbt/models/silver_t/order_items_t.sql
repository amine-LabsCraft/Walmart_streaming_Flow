{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='merge'
    )
}}

SELECT
    source_data.*,
    current_timestamp() AS processed_at

FROM {{ source('walmart_databricks', 'order_items') }} AS source_data

{% if is_incremental() %}
WHERE source_data.change_version > (
    SELECT COALESCE(
        MAX(target_data.change_version),
        0
    )
    FROM {{ this }} AS target_data
)
{% endif %}