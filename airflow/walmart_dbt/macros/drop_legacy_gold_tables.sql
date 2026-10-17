{#
    Lists, then optionally drops, the Gold tables left over from the previous
    model (SCD2 snapshots used as dimensions and manual backups). Nothing is
    dropped unless apply=true:

        dbt run-operation drop_legacy_gold_tables
        dbt run-operation drop_legacy_gold_tables --args '{apply: true}'
#}
{% macro drop_legacy_gold_tables(apply=false) %}
    {% set legacy_tables = [
        'dim_customers', 'dim_employees', 'dim_products', 'dim_stores', 'dim_orders',
        'dim_orders_backup_pre_grain_fix_20260730',
        'dim_employees_backup_pre_null_cleanup_20260801'
    ] %}
    {% for table in legacy_tables %}
        {% if apply %}
            {% do run_query('DROP TABLE IF EXISTS walmart.gold.' ~ table) %}
            {{ log('Dropped walmart.gold.' ~ table, info=True) }}
        {% else %}
            {{ log('Would drop walmart.gold.' ~ table ~ ' (pass apply: true)', info=True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
