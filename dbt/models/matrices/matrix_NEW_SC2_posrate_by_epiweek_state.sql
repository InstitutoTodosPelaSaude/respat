{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" ELSE 0 END) * 100 AS "SC2"
    FROM {{ ref("matrix_02_epiweek_state") }}
    GROUP BY epiweek_enddate, state_code
)
SELECT
    epiweek_enddate as "semanas_epidemiologicas",
    state_code as "UF",
    "SC2" as "percentual"
FROM source_data
WHERE "SC2" > 0
ORDER BY epiweek_enddate, state_code
    