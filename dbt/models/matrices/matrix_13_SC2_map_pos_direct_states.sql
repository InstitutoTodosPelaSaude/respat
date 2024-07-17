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
),
source_data_sum AS (
    SELECT
        epiweek_enddate as "semanas epidemiologicas",
        state_code as "state",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) as "cases"
    FROM source_data
    WHERE state_code IS NOT NULL
    GROUP BY epiweek_enddate, state_code
),
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        "state",
        SUM("cases") OVER (PARTITION BY "state" ORDER BY "semanas epidemiologicas") as "cases"
    FROM source_data_sum
    ORDER BY "semanas epidemiologicas", "state"
)
SELECT
    "semanas epidemiologicas",
    "state",
    "cases"
FROM source_data_cumulative_sum
WHERE "cases" > 0