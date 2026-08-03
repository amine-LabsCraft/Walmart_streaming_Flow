{% macro backfill_dim_order_item_id(min_order_id=10001) %}
    {% if execute %}
        {% set backfill_sql %}
            MERGE INTO walmart.gold.dim_orders AS target
            USING (
                SELECT
                    order_id,
                    MIN(order_item_id) AS order_item_id
                FROM walmart.silver_b.obt_b
                WHERE order_id >= {{ min_order_id }}
                  AND order_item_id IS NOT NULL
                GROUP BY order_id
            ) AS source
            ON target.order_id = source.order_id
               AND CAST(target.dbt_valid_to AS DATE) = DATE '9999-12-31'
            WHEN MATCHED THEN UPDATE SET
                target.order_item_id = source.order_item_id
        {% endset %}
        {% do run_query(backfill_sql) %}

        {% set columns_sql %}
            SELECT column_name
            FROM walmart.information_schema.columns
            WHERE table_schema = 'gold'
              AND table_name = 'dim_orders'
              AND column_name IN ('order_item_ids', 'order_item_count')
            ORDER BY column_name
        {% endset %}
        {% set extra_columns = run_query(columns_sql).columns[0].values() %}

        {% if extra_columns | length > 0 %}
            {% do run_query(
                "ALTER TABLE walmart.gold.dim_orders SET TBLPROPERTIES "
                ~ "('delta.columnMapping.mode' = 'name')"
            ) %}

            {% set quoted_extra_columns = [] %}
            {% for column_name in extra_columns %}
                {% do quoted_extra_columns.append(adapter.quote(column_name)) %}
            {% endfor %}

            {% do run_query(
                "ALTER TABLE walmart.gold.dim_orders DROP COLUMNS ("
                ~ quoted_extra_columns | join(', ')
                ~ ")"
            ) %}
        {% endif %}
    {% endif %}
{% endmacro %}
