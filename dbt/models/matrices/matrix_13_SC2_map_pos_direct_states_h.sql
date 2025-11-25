{{ config(materialized='table') }}

{% set epiweek_start = '2025-06-14' %}

-- CTE para listar todas as semanas epidemiológicas a partir de uma data inicial
WITH epiweeks AS (
    SELECT DISTINCT
        epiweek_enddate
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE epiweek_enddate >= '{{ epiweek_start }}'
),

WITH population AS (
    SELECT
        "DS_UF_SIGLA" as state_code,
        sum("Populacao") as population_qty
    FROM {{ ref("macroregions") }}
    GROUP BY "DS_UF_SIGLA"
),

-- CTE para listar todos os estados presentes nos dados
states AS (
    SELECT DISTINCT
        source.state_code,
        state as "state_name",
        population_qty
    FROM {{ ref("matrices_01_unpivot_combined") }} AS source
    LEFT JOIN population USING (state_code)
    WHERE state_code IS NOT NULL
),

-- CTE que gera todas as combinações de semanas epidemiológicas e estados
epiweeks_states AS (
    SELECT
        e.epiweek_enddate,
        s.state_code,
        s.state_name,
        s.population_qty
    FROM epiweeks e
    CROSS JOIN states s
),

-- CTE que filtra e estrutura os dados de origem, excluindo certos kits de teste
source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'sc2_antigen', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, state_code, pathogen
),

-- CTE que calcula a soma de casos por combinação de semana e estado, garantindo que
-- todas as combinações sejam representadas, mesmo que o número de casos seja zero
source_data_sum AS (
    SELECT
        e.epiweek_enddate as "semanas epidemiologicas",
        e.state_name as "state",
        e.state_code as "state_code",
        e.population_qty,
        COALESCE(SUM(CASE WHEN s.pathogen = 'SC2' THEN s."Pos" ELSE 0 END), 0) as "cases"
    FROM epiweeks_states e
    LEFT JOIN source_data s 
    ON e.epiweek_enddate = s.epiweek_enddate 
    AND e.state_code = s.state_code
    GROUP BY e.epiweek_enddate, e.state_code, e.state_name, e.population_qty
),

-- CTE que calcula a soma cumulativa de casos por estado, ordenando por semana
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        "state",
        "state_code",
        "cases" AS "epiweek_cases",
        "population_qty",
        SUM("cases") OVER (PARTITION BY "state_code" ORDER BY "semanas epidemiologicas") as "cumulative_cases"
    FROM source_data_sum
    ORDER BY "semanas epidemiologicas", "state_code"
)

-- Seleção final das colunas desejadas, ordenada por semana e estado
SELECT
    "semanas epidemiologicas",
    "state_code",
    "state",
    "population_qty" as "População",
    "epiweek_cases"::int AS "Casos da semana",
    "cumulative_cases"::int AS "Casos acumulados",
    "cumulative_cases"::float / NULLIF("population_qty", 0) * 100000 AS "Casos por 100 mil hab."
FROM source_data_cumulative_sum
WHERE "cumulative_cases" > 0 AND state <> 'NOT REPORTED'
ORDER BY "semanas epidemiologicas", "state_code"
