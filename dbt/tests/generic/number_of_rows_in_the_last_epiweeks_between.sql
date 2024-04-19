{% test number_of_rows_in_the_last_epiweeks_between(model, min_value, max_value) %}

WITH latest_epiweek AS (
    SELECT
        *
    FROM epiweeks
    WHERE epiweeks.start_date >= CURRENT_DATE - INTERVAL '30 days'
    AND   epiweeks.start_date  <= CURRENT_DATE
),
model_group_by_date_testing AS (
    SELECT 
        date_testing, 
        COUNT(*) AS test_count
    FROM {{ model }}
    GROUP BY date_testing
),
model_test_count_by_epiweek AS (
    SELECT
        latest_epiweek.end_date as epiweek_end_date,
        latest_epiweek.start_date as epiweek_start_date,
        latest_epiweek.week_num as epiweek_number,

        COALESCE(SUM(model_group_by_date_testing.test_count), 0)
        AS test_count

    FROM
        model_group_by_date_testing
    RIGHT JOIN 
        latest_epiweek
    ON 
        model_group_by_date_testing.date_testing >= latest_epiweek.start_date AND 
        model_group_by_date_testing.date_testing <= latest_epiweek.end_date
    GROUP BY 1,2,3
)
SELECT
    *
FROM model_test_count_by_epiweek
WHERE NOT (test_count >= {{ min_value }} AND test_count <= {{ max_value }})

{% endtest %}