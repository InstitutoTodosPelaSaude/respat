{{ config(materialized='table') }}

{% set historical_fill_threshold = 0.7 %} 
{% set last_year_fill_threshold = 0.7 %} 
{% set last_year_days_threshold = 381 %} -- 55 weeks

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    GROUP BY epiweek_enddate, state_code, region, pathogen
    ORDER BY epiweek_enddate, state_code, region, pathogen
),

states_filtered AS (
    SELECT
        epiweek_enddate,
        state_code,
        region,
        MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END)  AS "percentual",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END)::int         AS "pos",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Neg" ELSE 0 END)::int         AS "neg"
    FROM source_data
    GROUP BY epiweek_enddate, state_code, region
),

last_epiweek AS (
    SELECT MAX(epiweek_enddate) as epiweek_enddate
    FROM states_filtered
),

total_historical_weeks AS (
    SELECT 
        COUNT(DISTINCT epiweek_enddate) as total
    FROM states_filtered
),

total_last_year_weeks AS (
    SELECT 
        COUNT(DISTINCT epiweek_enddate) as total
    FROM states_filtered
    WHERE epiweek_enddate >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}
),

historical_completion_percentage_per_state AS (
    SELECT
        state_code,
        COUNT(CASE WHEN "percentual" IS NOT NULL THEN 1 ELSE NULL END) AS filled
    FROM states_filtered
    GROUP BY state_code
),

last_year_completion_percentage_per_state AS (
    SELECT
        state_code,
        COUNT(CASE WHEN "percentual" IS NOT NULL THEN 1 ELSE NULL END) AS filled
    FROM states_filtered
    WHERE epiweek_enddate >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}
    GROUP BY state_code
)

SELECT
    epiweek_enddate         AS "semanas_epidemiologicas",
    state_code              AS "UF",
    region                  AS "Região",
    "percentual"            AS "Positividade",
    ("pos" + "neg")::int    AS "Total de testes realizados",
    "pos"                   AS "Total de positivos",
    "neg"                   AS "Total de negativos",

    -- Historical completion
    CASE 
        WHEN historical_completion.filled::decimal / total_historical_weeks.total >= {{ historical_fill_threshold }} THEN 'Bom Histórico'
        ELSE 'Ruim histórico'
    END AS "Completude do histórico",
    historical_completion.filled::decimal / total_historical_weeks.total AS "Valor da completude do histórico",

    -- Last year completion
    CASE 
        WHEN year_completion.filled::decimal / total_last_year_weeks.total >= {{ last_year_fill_threshold }} THEN 'Bom 12 meses'
        ELSE 'Ruim 12 meses'
    END AS "Completude do ano",
    year_completion.filled::decimal / total_last_year_weeks.total AS "Valor da completude do ano",

    -- Final completion
    CASE 
        WHEN 
            historical_completion.filled::decimal / total_historical_weeks.total >= {{ historical_fill_threshold }} 
            AND year_completion.filled::decimal / total_last_year_weeks.total >= {{ last_year_fill_threshold }} 
        THEN 'Alta qualidade'
        WHEN
            historical_completion.filled::decimal / total_historical_weeks.total < {{ historical_fill_threshold }}
            AND year_completion.filled::decimal / total_last_year_weeks.total < {{ last_year_fill_threshold }}
        THEN 'Baixa qualidade'
        WHEN
            historical_completion.filled::decimal / total_historical_weeks.total >= {{ historical_fill_threshold }}
            AND year_completion.filled::decimal / total_last_year_weeks.total < {{ last_year_fill_threshold }}
        THEN 'Apenas histórico'
        ELSE 'Apenas 12 meses'
    END AS "Completude final",

    -- Raw metrics
    total_historical_weeks.total::int AS "Total de semanas históricas",
    historical_completion.filled::int AS "Semanas históricas preenchidas",
    total_last_year_weeks.total::int AS "Total de semanas do ano",
    year_completion.filled::int AS "Semanas do ano preenchidas"
FROM states_filtered
LEFT JOIN historical_completion_percentage_per_state as historical_completion USING (state_code)
LEFT JOIN last_year_completion_percentage_per_state as year_completion USING (state_code)
CROSS JOIN total_historical_weeks
CROSS JOIN total_last_year_weeks
WHERE 
    "state_code" <> 'NOT REPORTED' AND
    "region" <> 'NOT REPORTED'
ORDER BY epiweek_enddate, state_code