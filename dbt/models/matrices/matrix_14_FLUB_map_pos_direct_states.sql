{{ config(materialized='table') }}

{% set epiweek_start = '2024-05-19' %}

-- CTE para listar todas as semanas epidemiológicas a partir de uma data inicial
WITH epiweeks AS (
    SELECT DISTINCT
        epiweek_enddate
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE epiweek_enddate >= '{{ epiweek_start }}'
),

-- CTE para listar todos os estados presentes nos dados
states AS (
    SELECT DISTINCT
        state_code
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE state_code IS NOT NULL
),

-- CTE que gera todas as combinações de semanas epidemiológicas e estados
epiweeks_states AS (
    SELECT
        e.epiweek_enddate,
        s.state_code
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
        test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, state_code, pathogen
),

-- CTE que calcula a soma de casos por combinação de semana e estado, garantindo que
-- todas as combinações sejam representadas, mesmo que o número de casos seja zero
source_data_sum AS (
    SELECT
        e.epiweek_enddate as "semanas epidemiologicas",
        e.state_code as "state",
        COALESCE(SUM(CASE WHEN s.pathogen = 'FLUB' THEN s."Pos" ELSE 0 END), 0) as "cases"
    FROM epiweeks_states e
    LEFT JOIN source_data s 
    ON e.epiweek_enddate = s.epiweek_enddate 
    AND e.state_code = s.state_code
    GROUP BY e.epiweek_enddate, e.state_code
),

-- CTE que calcula a soma cumulativa de casos por estado, ordenando por semana
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        "state",
        "cases" AS "epiweek_cases",
        SUM("cases") OVER (PARTITION BY "state" ORDER BY "semanas epidemiologicas") as "cumulative_cases"
    FROM source_data_sum
    ORDER BY "semanas epidemiologicas", "state"
)

-- Seleção final das colunas desejadas, ordenada por semana e estado
SELECT
    "semanas epidemiologicas",
    "state",
    "epiweek_cases",
    "cumulative_cases"
FROM source_data_cumulative_sum
WHERE "cumulative_cases" > 0
ORDER BY "semanas epidemiologicas", "state"
