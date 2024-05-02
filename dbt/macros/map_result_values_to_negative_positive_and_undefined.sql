{% macro 
    map_result_values_to_negative_positive_and_undefined(
        column_name,
        min_value, max_value, 
        INDETERMINADO,
        NAO_RECONHECIDO
    ) 
%}
    CASE 
        WHEN {{column_name}} ~ '[0-9]+[.]*[0-9]*' AND {{column_name}} ~ '^[0-9]' THEN
            CASE
                WHEN {{column_name}}::FLOAT <= {{ min_value }} THEN 0
                WHEN {{column_name}}::FLOAT > {{ min_value }} AND {{column_name}}::FLOAT <= {{ max_value }} THEN {{ INDETERMINADO }}
                WHEN {{column_name}}::FLOAT > {{ max_value }} THEN 1
            END
        ELSE {{ NAO_RECONHECIDO }}
    END
{% endmacro %}