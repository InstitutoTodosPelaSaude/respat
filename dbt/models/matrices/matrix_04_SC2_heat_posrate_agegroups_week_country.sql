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
        test_kit IN ('covid_antigen', 'covid_pcr', 'sc2_antigen', 'thermo', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
),

sc2_data AS (
    SELECT
        age_group AS "faixas etárias",
        epiweek_enddate AS "semana epidemiológica",
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "percentual",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END)::int    AS "pos",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Neg" ELSE 0 END)::int    AS "neg"
    FROM source_data
    GROUP BY epiweek_enddate, age_group
)

SELECT
    "semana epidemiológica" AS "Semana epidemiológica",
    "faixas etárias"        AS "Faixa etária",
    "percentual"            AS "Taxa de positividade",
    ("pos" + "neg")::int    AS "Total de testes realizados",
    "pos"                   AS "Total de positivos",
    "neg"                   AS "Total de negativos"
FROM sc2_data
ORDER BY "semana epidemiológica", "faixas etárias" DESC
    