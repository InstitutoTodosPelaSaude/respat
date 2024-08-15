{{ config(materialized='table') }}

{% set state_fill_threshold = 0.7 %} 

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    GROUP BY epiweek_enddate, state_code, pathogen
    ORDER BY epiweek_enddate, state_code, pathogen
),

states_above_threshold AS (
    SELECT distinct state_code
    FROM (
        SELECT
            state_code,
            MAX(total) as total,
            COUNT(CASE WHEN "posrate" IS NOT NULL AND "posrate" > 0 THEN 1 ELSE NULL END) AS filled
        FROM 
            source_data, 
            (SELECT COUNT(DISTINCT epiweek_enddate) as total FROM source_data)
        WHERE pathogen = 'SC2'
        GROUP BY state_code
    ) AS state_fill_rates
    WHERE filled::decimal / total >= {{ state_fill_threshold }}
),

states_filtered AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "SC2"
    FROM source_data
    WHERE state_code IN (SELECT state_code FROM states_above_threshold)
    GROUP BY epiweek_enddate, state_code
)

SELECT
    epiweek_enddate as "semanas_epidemiologicas",
    state_code as "UF",
    "SC2" as "percentual"
FROM states_filtered
WHERE "SC2" is not null
ORDER BY epiweek_enddate, state_code