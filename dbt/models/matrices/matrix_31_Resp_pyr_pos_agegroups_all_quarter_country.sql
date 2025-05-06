{{ config(materialized='table') }}

{% set date_testing_start = '2022-01-01' %}

WITH source_data AS (
    SELECT
        TO_CHAR("date_testing", 'YYYY - Q"º Trimestre"') AS trimestre,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        test_kit IN ('test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24') AND 
        age_group != 'NOT REPORTED' AND
        date_testing >= '{{ date_testing_start }}' AND
        date_testing < DATE_TRUNC('quarter', CURRENT_DATE)
    GROUP BY trimestre, age_group, pathogen
),
faixas_etarias AS (
    SELECT DISTINCT age_group FROM source_data
),
trimestres AS (
    SELECT DISTINCT trimestre FROM source_data
),
combinacoes AS (
    SELECT t.trimestre, f.age_group
    FROM trimestres t
    CROSS JOIN faixas_etarias f
),
aggregated_data AS (
    SELECT
        trimestre,
        age_group AS "faixas_etarias",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END)::int      AS "SARS-CoV-2",
        SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END)::int     AS "Influenza A",
        SUM(CASE WHEN pathogen = 'FLUB' THEN "Pos" ELSE 0 END)::int     AS "Influenza B",
        SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END)::int      AS "Vírus Sincicial Respiratório",
        SUM(CASE WHEN pathogen = 'COVS' THEN "Pos" ELSE 0 END)::int     AS "Coronavírus sazonais",
        SUM(CASE WHEN pathogen = 'ADENO' THEN "Pos" ELSE 0 END)::int    AS "Adenovírus",
        SUM(CASE WHEN pathogen = 'BOCA' THEN "Pos" ELSE 0 END)::int     AS "Bocavírus",
        SUM(CASE WHEN pathogen = 'RINO' THEN "Pos" ELSE 0 END)::int     AS "Rinovírus",
        SUM(CASE WHEN pathogen = 'PARA' THEN "Pos" ELSE 0 END)::int     AS "Vírus Parainfluenza",
        SUM(CASE WHEN pathogen = 'ENTERO' THEN "Pos" ELSE 0 END)::int   AS "Enterovírus",
        SUM(CASE WHEN pathogen = 'META' THEN "Pos" ELSE 0 END)::int     AS "Metapneumovírus",
        SUM(CASE WHEN pathogen = 'BAC' THEN "Pos" ELSE 0 END)::int      AS "Bactérias"
    FROM source_data
    GROUP BY trimestre, age_group
)
SELECT
    c.trimestre,
    c.age_group AS "faixas_etarias",
    COALESCE(a."SARS-CoV-2"::int, null)::int AS "SARS-CoV-2",
    COALESCE(a."Influenza A"::int, null)::int AS "Influenza A",
    COALESCE(a."Influenza B"::int, null)::int AS "Influenza B",
    COALESCE(a."Vírus Sincicial Respiratório"::int, null)::int AS "Vírus Sincicial Respiratório",
    COALESCE(a."Coronavírus sazonais"::int, null)::int AS "Coronavírus sazonais",
    COALESCE(a."Adenovírus"::int, null)::int AS "Adenovírus",
    COALESCE(a."Bocavírus"::int, null)::int AS "Bocavírus",
    COALESCE(a."Rinovírus"::int, null)::int AS "Rinovírus",
    COALESCE(a."Vírus Parainfluenza"::int, null)::int AS "Vírus Parainfluenza",
    COALESCE(a."Enterovírus"::int, null)::int AS "Enterovírus",
    COALESCE(a."Metapneumovírus"::int, null)::int AS "Metapneumovírus",
    COALESCE(a."Bactérias"::int, null)::int AS "Bactérias"
FROM combinacoes c
LEFT JOIN aggregated_data a
    ON c.trimestre = a.trimestre AND c.age_group = a.faixas_etarias
ORDER BY c.trimestre ASC, c.age_group DESC