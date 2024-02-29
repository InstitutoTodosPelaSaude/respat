{% macro normalize_text(column) %}
    TRIM( BOTH '\t' FROM TRIM(UPPER(unaccent("{{ column }}"))))
{% endmacro %}