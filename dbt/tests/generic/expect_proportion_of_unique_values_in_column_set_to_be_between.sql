{% test expect_proportion_of_unique_values_in_column_set_to_be_between(model, columns, min_value, max_value) %}

    SELECT
        proportion_of_unique_values
    FROM
    (
        SELECT 
            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT {{ columns }} 
                    FROM {{ model }}
                )
            )::FLOAT / COUNT(*)::FLOAT AS proportion_of_unique_values
        FROM {{ model }}
    )
    WHERE proportion_of_unique_values < {{ min_value }} OR proportion_of_unique_values > {{ max_value }}

{% endtest %}
