{% macro 
    map_result_values_to_negative_and_positive(
        column_name,
        threshold,
        NAO_RECONHECIDO,
        result_is_numeric = "result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]'"
    ) 
%}
    CASE 
        WHEN {{ result_is_numeric }} THEN
            CASE
                WHEN {{column_name}}::FLOAT < {{ threshold }} THEN 0
                ELSE 1
            END
        ELSE {{ NAO_RECONHECIDO }}
    END
{% endmacro %}