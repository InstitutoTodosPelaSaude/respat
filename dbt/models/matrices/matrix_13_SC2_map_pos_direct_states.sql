{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        test_kit NOT IN ('sc2_igg') AND
        epiweek_enddate >= '2024-05-19'
    GROUP BY epiweek_enddate, state_code, pathogen
    ORDER BY epiweek_enddate, state_code, pathogen
)
SELECT
    epiweek_enddate as "semanas epidemiologicas",
    state_code as "state",
    SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) as "cases"
FROM source_data
WHERE state_code IS NOT NULL
GROUP BY epiweek_enddate, state_code
ORDER BY epiweek_enddate, state_code