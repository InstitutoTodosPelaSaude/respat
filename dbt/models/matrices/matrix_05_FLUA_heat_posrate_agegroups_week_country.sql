{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
),

flua_data AS (
    SELECT
        'Pos' AS "FLUA_test_result",
        age_group AS "faixas etárias",
        epiweek_enddate AS "semana epidemiológica",
        MAX(CASE WHEN pathogen = 'FLUA' THEN "posrate" * 100 ELSE NULL END) AS "percentual"
    FROM source_data
    GROUP BY epiweek_enddate, age_group
)

SELECT
    "FLUA_test_result",
    "faixas etárias",
    "semana epidemiológica",
    "percentual"
FROM flua_data
WHERE "faixas etárias" <> 'NOT REPORTED'
ORDER BY "semana epidemiológica", "faixas etárias"
    