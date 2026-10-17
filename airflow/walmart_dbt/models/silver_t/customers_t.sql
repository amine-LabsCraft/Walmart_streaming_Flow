{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_id',
        incremental_strategy = 'merge'
    )
}}

SELECT
    source_data.*,
    current_timestamp() AS processed_at

FROM {{ source('walmart_databricks', 'customers') }} AS source_data

{% if is_incremental() %}
{{ incremental_cdc_filter('updated_timestamp') }}
{% endif %}
