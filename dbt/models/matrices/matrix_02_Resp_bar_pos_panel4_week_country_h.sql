{{ config(materialized='table') }}

{% set epiweek_start = '2022-02-26' %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        CASE
            WHEN "SC2_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_4', 'test_21', 'test_24')
            WHEN "FLUA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_4', 'test_21', 'test_24')
            WHEN "FLUB_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_4', 'test_21', 'test_24')
            WHEN "VSR_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_4', 'test_21', 'test_24')
            ELSE FALSE
        END AND 
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)
SELECT
    epiweek_enddate as "semana epidemiológica",
    SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END)::int AS "Influenza A",
    SUM(CASE WHEN pathogen = 'FLUB' THEN "Pos" ELSE 0 END)::int AS "Influenza B",
    SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END)::int  AS "SARS-CoV-2",
    SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END)::int  AS "Vírus Sincicial Respiratório"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    