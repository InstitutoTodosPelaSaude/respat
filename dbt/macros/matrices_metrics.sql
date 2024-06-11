{% macro matrices_metrics(result_column) %}
    SUM(CASE WHEN {{ result_column }} = 'Pos' THEN 1 ELSE 0 END) AS "Pos",
    SUM(CASE WHEN {{ result_column }} = 'Neg' THEN 1 ELSE 0 END) AS "Neg",
    SUM(CASE WHEN {{ result_column }} IN ('Pos', 'Neg') THEN 1 ELSE 0 END) AS "totaltests",
    CASE
        -- Only compute positivity rate if there are more than 50 tests
        WHEN SUM(CASE WHEN {{ result_column }} IN ('Pos', 'Neg') THEN 1 ELSE 0 END) > 50 THEN
            SUM(CASE WHEN {{ result_column }} = 'Pos' THEN 1 ELSE 0 END)::decimal / SUM(CASE WHEN {{ result_column }} IN ('Pos', 'Neg') THEN 1 ELSE 0 END)
        ELSE NULL
    END AS "posrate"
{% endmacro %}