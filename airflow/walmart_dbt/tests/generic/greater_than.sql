{% test greater_than(model, column_name, value) %}

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND {{ column_name }} <= {{ value }}

{% endtest %}