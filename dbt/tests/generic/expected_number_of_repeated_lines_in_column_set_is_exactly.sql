{% test expected_number_of_repeated_lines_in_column_set_is_exactly(model, columns, values_list) %}

    
    SELECT COUNT(*), {{ columns }}
    FROM {{ model }}
    GROUP BY {{ columns }}
    HAVING COUNT(*) NOT IN ({{ values_list | join(', ') }})
    
{% endtest %}