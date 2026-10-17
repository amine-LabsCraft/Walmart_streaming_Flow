{#
    Incremental filter shared by the silver_t models.

    The watermark is the highest cursor already loaded in the target, minus a
    small overlap. A source transaction can commit after a newer one (its
    change_version or updated_timestamp is assigned before COMMIT), so a row
    may land in bronze below the current watermark. Re-reading the overlap
    catches it; the merge on unique_key makes the re-read rows harmless.
#}
{% macro incremental_cdc_filter(cursor_column) %}
{%- if cursor_column == 'change_version' %}
WHERE source_data.change_version > (
    SELECT COALESCE(MAX(target_data.change_version), 0)
        - {{ var('cdc_overlap_versions') }}
    FROM {{ this }} AS target_data
)
{%- else %}
WHERE source_data.{{ cursor_column }} >= (
    SELECT COALESCE(
        MAX(target_data.{{ cursor_column }}),
        CAST('1900-01-01' AS TIMESTAMP)
    ) - INTERVAL {{ var('cdc_overlap_minutes') }} MINUTES
    FROM {{ this }} AS target_data
)
{%- endif %}
{% endmacro %}
