{% macro 
    map_result_values_to_negative_and_positive(
        column_name,
        threshold,
        NAO_RECONHECIDO
    ) 
%}
    CASE 
        WHEN {{column_name}} ~ '[0-9]+[.]*[0-9]*' AND {{column_name}} ~ '^[0-9]' THEN
            CASE
                WHEN {{column_name}}::FLOAT < {{ threshold }} THEN 0
                ELSE 1
            END
        ELSE {{ NAO_RECONHECIDO }}
    END
{% endmacro %}