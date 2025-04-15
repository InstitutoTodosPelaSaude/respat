{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-01' %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        age_group NOT IN ('NOT REPORTED') AND
        test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
),

flua_data AS (
    SELECT
        age_group AS "faixas etárias",
        epiweek_enddate AS "semana epidemiológica",
        MAX(CASE WHEN pathogen = 'FLUA' THEN "posrate" * 100 ELSE NULL END) AS "percentual",
        SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END)::int    AS "pos",
        SUM(CASE WHEN pathogen = 'FLUA' THEN "Neg" ELSE 0 END)::int    AS "neg"
    FROM source_data
    GROUP BY epiweek_enddate, age_group
)

SELECT
    "semana epidemiológica" AS "Semana epidemiológica",
    "faixas etárias"          AS "Faixa etária",
    "percentual"            AS "Positividade",
    ("pos" + "neg")::int    AS "Total de testes realizados",
    "pos"                   AS "Total de positivos",
    "neg"                   AS "Total de negativos"
FROM flua_data
ORDER BY "semana epidemiológica", "faixas etárias" DESC
    