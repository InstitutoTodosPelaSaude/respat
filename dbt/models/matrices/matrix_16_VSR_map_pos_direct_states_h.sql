{{ config(materialized='table') }}

{% set epiweek_start = '2024-11-02' %}

-- CTE para selecionar todas as datas finais de semana epidemiológica
WITH epiweeks AS (
    SELECT DISTINCT
        epiweek_enddate
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        epiweek_enddate >= '{{ epiweek_start }}' 
),

-- CTE para selecionar os dados de origem relevantes para cada semana epidemiológica
source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        state,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        "VSR_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('vsr_antigen', 'test_4', 'test_3', 'test_14', 'test_23', 'test_21', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, state_code, state, pathogen
),

-- CTE para obter dados únicos de estado (codigo_estado, estado)
state_data AS (
    SELECT DISTINCT
        state_code,
        state
    FROM source_data
),

-- CTE que cria uma combinação de todas as semanas epidemiológicas com todos os estados
epiweeks_states AS (
    SELECT
        e.epiweek_enddate,
        l.state_code,
        l.state
    FROM epiweeks e
    CROSS JOIN state_data l
),

-- CTE que calcula a soma de casos por semana epidemiológica e estado
-- Inclui semanas e estados sem casos usando COALESCE para garantir que zeros sejam registrados
source_data_sum AS (
    SELECT
        e.epiweek_enddate as "semanas epidemiologicas",
        e.state_code as "state_code",
        e.state as "state",
        COALESCE(SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END), 0) as "cases"
    FROM epiweeks_states e
    LEFT JOIN source_data s ON e.epiweek_enddate = s.epiweek_enddate 
                             AND e.state = s.state
    GROUP BY e.epiweek_enddate, e.state_code, e.state
),

population AS (
    SELECT
        "DS_UF_SIGLA" as state_code,
        sum("Populacao"::int) as population_qty
    FROM {{ ref("macroregions") }}
    where "ADM2_PCODE" ilike 'BR%'
    GROUP BY "DS_UF_SIGLA"
),

-- CTE que calcula a soma cumulativa dos casos para cada estado
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        source_data_sum."state_code",
        "state",
        "cases" AS "epiweek_cases",
        population."population_qty",
        SUM("cases") OVER (PARTITION BY source_data_sum."state_code" ORDER BY "semanas epidemiologicas") as "cumulative_cases"
    FROM source_data_sum
    LEFT JOIN population ON source_data_sum."state_code" = population.state_code
    ORDER BY "semanas epidemiologicas", source_data_sum."state_code"
)

-- Seleção final dos dados, filtrando apenas semanas com casos cumulativos maiores que zero
SELECT
    "semanas epidemiologicas" as "Semanas epidemiológicas",
    "state_code" as "State_code",
    "state" as "State",
    "population_qty" as "População",
    "epiweek_cases"::INTEGER as "Casos na última semana",
    "cumulative_cases"::INTEGER as "Casos cumulativos",
    "cumulative_cases"::float / NULLIF("population_qty", 0) * 100000 AS "Casos por 100 mil hab."
FROM source_data_cumulative_sum
WHERE 
    "cumulative_cases" > 0 AND
    state not in ('NOT REPORTED')
ORDER BY "state_code", "semanas epidemiologicas"
    