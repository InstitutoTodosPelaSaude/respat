{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        MAX(CASE WHEN pathogen = 'FLUA' THEN "posrate" * 100 ELSE NULL END) AS "FLUA"
    FROM {{ ref("matrix_02_epiweek_pathogen_agegroup") }}
    GROUP BY epiweek_enddate, age_group
)
SELECT
    'Pos' as "FLUA_test_result",
    age_group as "faixas etárias",
    epiweek_enddate as "semana epidemiológica",
    "FLUA" AS "percentual"
FROM source_data
WHERE age_group <> 'NOT REPORTED'
ORDER BY epiweek_enddate, age_group
    