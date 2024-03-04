{% test qty_original_lines_equals_row_count(model, column_name, ref) %}
 
    WITH source_data AS (
    SELECT * FROM
        {{ ref }}
    ),
    source_deduplicated AS (
        SELECT * FROM 
        {{ model }}
    ),
    diff_qty_lines_result AS (
        SELECT
            SUM({{column_name}}) - (SELECT COUNT(*) FROM source_data) AS diff_qty_lines
        FROM source_deduplicated 
    )
    SELECT 
        * 
    FROM diff_qty_lines_result
    WHERE diff_qty_lines > 0
    
{% endtest %}