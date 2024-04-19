{% test expect_number_of_repeated_lines_in_column_set_to_be_less_than(model, columns, max_value) %}

    SELECT
        COUNT(*)
    FROM
    (
        SELECT COUNT(*), {{ columns }}
        FROM {{ model }}
        GROUP BY {{ columns }}
        HAVING COUNT(*) > 1
    ) _
    HAVING COUNT(*) > {{ max_value }} 

{% endtest %}