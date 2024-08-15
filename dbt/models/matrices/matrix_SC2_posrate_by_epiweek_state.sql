{{ config(materialized='table') }}

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

sc2_posrate AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "SC2"
    FROM source_data
    GROUP BY epiweek_enddate, state_code
)

SELECT
    epiweek_enddate as "semanas_epidemiologicas",
    state_code as "UF",
    "SC2" as "percentual"
FROM sc2_posrate
WHERE "SC2" is not null
ORDER BY epiweek_enddate, state_code
    