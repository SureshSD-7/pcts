{% macro requiredcolumn(sourcename,tablename) %}
    SELECT * FROM {{ source(sourcename, tablename) }}
{% endmacro %}