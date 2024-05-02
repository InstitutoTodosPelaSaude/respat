{% macro 
    map_result_values_to_negative_positive_and_undefined(
        min_value, max_value, 
        INDETERMINADO,
        NAO_RECONHECIDO,
        result_is_numeric = "result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]'"
    ) 
%}
    CASE 
        WHEN {{ result_is_numeric }} THEN
            CASE
                WHEN result::FLOAT <= {{ min_value }} THEN 0
                WHEN result::FLOAT > {{ min_value }} AND result::FLOAT <= {{ max_value }} THEN {{ INDETERMINADO }}
                WHEN result::FLOAT > {{ max_value }} THEN 1
            END
        ELSE {{ NAO_RECONHECIDO }}
    END
{% endmacro %}