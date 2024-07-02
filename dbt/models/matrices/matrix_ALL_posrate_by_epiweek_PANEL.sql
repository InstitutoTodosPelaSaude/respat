{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "SC2",
        MAX(CASE WHEN pathogen = 'FLUA' THEN "posrate" * 100 ELSE NULL END) AS "FLUA",
        MAX(CASE WHEN pathogen = 'FLUB' THEN "posrate" * 100 ELSE NULL END) AS "FLUB",
        MAX(CASE WHEN pathogen = 'VSR' THEN "posrate" * 100 ELSE NULL END) AS "VSR",
        MAX(CASE WHEN pathogen = 'COVS' THEN "posrate" * 100 ELSE NULL END) AS "COVS",
        MAX(CASE WHEN pathogen = 'ADENO' THEN "posrate" * 100 ELSE NULL END) AS "ADENO",
        MAX(CASE WHEN pathogen = 'BOCA' THEN "posrate" * 100 ELSE NULL END) AS "BOCA",
        MAX(CASE WHEN pathogen = 'RINO' THEN "posrate" * 100 ELSE NULL END) AS "RINO",
        MAX(CASE WHEN pathogen = 'PARA' THEN "posrate" * 100 ELSE NULL END) AS "PARA",
        MAX(CASE WHEN pathogen = 'ENTERO' THEN "posrate" * 100 ELSE NULL END) AS "ENTERO",
        MAX(CASE WHEN pathogen = 'META' THEN "posrate" * 100 ELSE NULL END) AS "META",
        MAX(CASE WHEN pathogen = 'BAC' THEN "posrate" * 100 ELSE NULL END) AS "BAC"
    FROM {{ ref("matrix_02_epiweek_pathogen_PANELOUTROS") }}
    GROUP BY epiweek_enddate
)
SELECT
    epiweek_enddate as "semana epidemiológica",
    "ADENO" AS "Adenovírus",
    "BAC" AS "Bactérias",
    "BOCA" AS "Bocavírus",
    "COVS" AS "Coronavírus sazonais",
    "ENTERO" AS "Enterovírus",
    "FLUA" AS "Influenza A",
    "FLUB" AS "Influenza B",
    "META" AS "Metapneumovírus",
    "PARA" AS "Vírus Parainfluenza",
    "RINO" AS "Rinovírus",
    "SC2" AS "SARS-CoV-2",
    "VSR" AS "Vírus Sincicial Respiratório"
FROM source_data
ORDER BY epiweek_enddate
    