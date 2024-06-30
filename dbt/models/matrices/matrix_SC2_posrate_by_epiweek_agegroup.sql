{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "SC2"
    FROM {{ ref("matrix_02_epiweek_pathogen_agegroup") }}
    GROUP BY epiweek_enddate, age_group
)
SELECT
    'Pos' as "SC2_test_result",
    age_group as "faixas etárias",
    epiweek_enddate as "semana epidemiológica",
    "SC2" AS "percentual"
FROM source_data
WHERE age_group <> 'NOT REPORTED'
ORDER BY epiweek_enddate, age_group
    