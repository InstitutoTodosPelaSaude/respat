{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) AS "SC2",
        SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END) AS "FLUA",
        SUM(CASE WHEN pathogen = 'FLUB' THEN "Pos" ELSE 0 END) AS "FLUB",
        SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END) AS "VSR",
        SUM(CASE WHEN pathogen = 'COVS' THEN "Pos" ELSE 0 END) AS "COVS",
        SUM(CASE WHEN pathogen = 'ADENO' THEN "Pos" ELSE 0 END) AS "ADENO",
        SUM(CASE WHEN pathogen = 'BOCA' THEN "Pos" ELSE 0 END) AS "BOCA",
        SUM(CASE WHEN pathogen = 'RINO' THEN "Pos" ELSE 0 END) AS "RINO",
        SUM(CASE WHEN pathogen = 'PARA' THEN "Pos" ELSE 0 END) AS "PARA",
        SUM(CASE WHEN pathogen = 'ENTERO' THEN "Pos" ELSE 0 END) AS "ENTERO",
        SUM(CASE WHEN pathogen = 'META' THEN "Pos" ELSE 0 END) AS "META",
        SUM(CASE WHEN pathogen = 'BAC' THEN "Pos" ELSE 0 END) AS "BAC"
    FROM {{ ref("matrix_02_epiweek_pathogen_PANELOUTROSv2") }}
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
    