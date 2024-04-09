{% test model_has_more_than_x_rows(model, n_rows) %}
 
    WITH source_data AS (
    SELECT * FROM
        {{ model }}
    )
    SELECT 
        1 AS _
    FROM source_data
    HAVING COUNT(*) < {{ n_rows }}
    
{% endtest %}