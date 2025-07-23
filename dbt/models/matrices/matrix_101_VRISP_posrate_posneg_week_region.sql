{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-01' %}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        CASE
            WHEN "SC2_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "FLUA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "FLUB_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "VSR_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_3', 'test_4', 'test_14', 'test_21', 'test_24', 'test_23', 'vsr_antigen')
            ELSE FALSE
        END AND
        epiweek_enddate >= '{{ epiweek_start }}'
        AND pathogen IN ('SC2', 'FLUA', 'FLUB', 'VSR')
    GROUP BY epiweek_enddate, region, pathogen
)

SELECT
    epiweek_enddate AS "Semanas epidemiológicas",
    region AS "Região",
    pathogen AS "Patógeno",
    "Pos" AS "Testes Positivos",
    "Neg" AS "Testes Negativos",
    "totaltests" AS "Total de Testes",
    "posrate" * 100 AS "Taxa de Positividade (%)"
FROM source_data
ORDER BY epiweek_enddate, region, pathogen

