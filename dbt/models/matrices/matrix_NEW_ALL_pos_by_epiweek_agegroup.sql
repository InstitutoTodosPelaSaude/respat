{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
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
    FROM {{ ref("matrix_02_epiweek_agegroup") }}
    GROUP BY epiweek_enddate, age_group
)
SELECT
    epiweek_enddate as "semana_epidemiológica",
    age_group as "faixas_etárias",
    "SC2" AS "SARS-CoV-2",
    "FLUA" AS "Influenza A",
    "FLUB" AS "Influenza B",
    "VSR" AS "Vírus Sincicial Respiratório",
    "COVS" AS "Coronavírus sazonais",
    "ADENO" AS "Adenovírus",
    "BOCA" AS "Bocavírus",
    "RINO" AS "Rinovírus",
    "PARA" AS "Vírus Parainfluenza",
    "ENTERO" AS "Enterovírus",
    "META" AS "Metapneumovírus",
    "BAC" AS "Bactérias"
FROM source_data
WHERE epiweek_enddate >= '2022-01-01'
ORDER BY epiweek_enddate, age_group
    