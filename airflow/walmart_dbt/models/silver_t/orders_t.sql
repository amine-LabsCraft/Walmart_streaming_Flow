{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

SELECT
    source_data.*,
    current_timestamp() AS processed_at

FROM {{ source('walmart_databricks', 'orders') }} AS source_data

{% if is_incremental() %}
{{ incremental_cdc_filter('change_version') }}
{% endif %}
