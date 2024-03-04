{% macro pivot_pathogen_results(pathogens, pathogen_source_column, result_column, pathogen_column_target) %}
    CASE
        {% for pathogen in pathogens %}
            WHEN {{ pathogen_source_column }} = '{{ pathogen }}' THEN {{ result_column }}
        {% endfor %}
        ELSE -1 -- -1 is the default value for no result, NT = Not Tested
    END AS "{{ pathogen_column_target }}"
{% endmacro %}