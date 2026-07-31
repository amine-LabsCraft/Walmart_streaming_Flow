{% macro repair_dim_orders_grain() %}
    {% if execute %}
        {% set backup_sql %}
            CREATE TABLE IF NOT EXISTS
                walmart.gold.dim_orders_backup_pre_grain_fix_20260730
            DEEP CLONE walmart.gold.dim_orders
        {% endset %}
        {% do run_query(backup_sql) %}

        {% set columns_sql %}
            SELECT column_name
            FROM walmart.information_schema.columns
            WHERE table_schema = 'gold'
              AND table_name = 'dim_orders'
            ORDER BY ordinal_position
        {% endset %}
        {% set column_rows = run_query(columns_sql) %}
        {% set quoted_columns = [] %}
        {% for row in column_rows %}
            {% do quoted_columns.append(adapter.quote(row[0])) %}
        {% endfor %}

        {% if quoted_columns | length == 0 %}
            {{ exceptions.raise_compiler_error('dim_orders does not exist') }}
        {% endif %}

        {% set repair_sql %}
            CREATE OR REPLACE TABLE walmart.gold.dim_orders AS
            SELECT {{ quoted_columns | join(', ') }}
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY dbt_scd_id
                        ORDER BY order_item_id
                    ) AS _grain_rank
                FROM walmart.gold.dim_orders
            )
            WHERE _grain_rank = 1
        {% endset %}
        {% do run_query(repair_sql) %}
    {% endif %}
{% endmacro %}
