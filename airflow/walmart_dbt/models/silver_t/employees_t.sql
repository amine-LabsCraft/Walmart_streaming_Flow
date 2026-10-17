{{
    config(
        materialized = 'incremental',
        unique_key = 'employee_id',
        incremental_strategy = 'merge'
    )
}}

SELECT
    source_data.*,
    current_timestamp() AS processed_at

FROM {{ source('walmart_databricks', 'employees') }} AS source_data

{% if is_incremental() %}
{{ incremental_cdc_filter('updated_timestamp') }}
{% endif %}
