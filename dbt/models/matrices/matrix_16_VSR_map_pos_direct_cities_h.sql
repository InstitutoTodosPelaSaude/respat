{{ config(materialized='table') }}

{% set epiweek_start = '2024-11-02' %}

-- CTE para selecionar todas as datas finais de semana epidemiológica
WITH epiweeks AS (
    SELECT DISTINCT
        epiweek_enddate
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE epiweek_enddate >= '{{ epiweek_start }}'
),

-- CTE para selecionar os dados de origem relevantes para cada semana epidemiológica
source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        state,
        location,
        location_ibge_code,
        lat,
        long,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        "VSR_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('vsr_antigen', 'test_4', 'test_3', 'test_14', 'test_23', 'test_21', 'test_24') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, state_code, state, location, location_ibge_code, lat, long, pathogen
    ORDER BY epiweek_enddate, state_code
),

-- CTE para obter dados únicos de localização (código IBGE, nome, estado, latitude, longitude)
location_data AS (
    SELECT DISTINCT
        location_ibge_code,
        location,
        state_code,
        state,
        lat,
        long
    FROM source_data
),

-- CTE que cria uma combinação de todas as semanas epidemiológicas com todas as localizações
epiweeks_locations AS (
    SELECT
        e.epiweek_enddate,
        l.location_ibge_code,
        l.location,
        l.state_code,
        l.state,
        l.lat,
        l.long
    FROM epiweeks e
    CROSS JOIN location_data l
),

-- CTE que calcula a soma de casos por semana epidemiológica e localização
-- Inclui semanas e localizações sem casos usando COALESCE para garantir que zeros sejam registrados
source_data_sum AS (
    SELECT
        e.epiweek_enddate as "semanas epidemiologicas",
        e.location_ibge_code as "location_ibge_code",
        e.location as "location",
        e.state_code as "state_code",
        e.state as "state",
        e.lat as "lat",
        e.long as "long",
        COALESCE(SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END), 0) as "cases"
    FROM epiweeks_locations e
    LEFT JOIN source_data s ON e.epiweek_enddate = s.epiweek_enddate 
                             AND e.location_ibge_code = s.location_ibge_code
    GROUP BY e.epiweek_enddate, e.location_ibge_code, e.location, e.state_code, e.state, e.lat, e.long
),

population AS (
    SELECT
        regexp_replace("ADM2_PCODE", '^BR', '')::int as location_ibge_code,
        "Populacao"::int as population_qty
    FROM {{ ref("macroregions") }}
    where "ADM2_PCODE" ilike 'BR%'
),

-- CTE que calcula a soma cumulativa dos casos para cada localização
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        source_data_sum."location_ibge_code",
        "location",
        "state_code",
        "state",
        "lat",
        "long",
        population."population_qty",
        "cases" AS "epiweek_cases",
        SUM("cases") OVER (PARTITION BY source_data_sum."location_ibge_code" ORDER BY "semanas epidemiologicas") as "cumulative_cases"
    FROM source_data_sum
    LEFT JOIN population ON source_data_sum."location_ibge_code" = population."location_ibge_code"
    ORDER BY "semanas epidemiologicas", "state_code", "location"
)

-- Seleção final dos dados, filtrando apenas semanas com casos cumulativos maiores que zero
SELECT
    "semanas epidemiologicas" as "Semanas epidemiologicas",
    "location_ibge_code" as "Location_ibge_code",
    "location" as "Location",
    "state_code" as "State_code",
    "state" as "State",
    "lat" as "Lat",
    "long" as "Long",
    "population_qty" as "População",
    "epiweek_cases"::INTEGER as "Casos na última semana",
    "cumulative_cases"::INTEGER as "Casos cumulativos",
    "cumulative_cases"::float / NULLIF("population_qty", 0) * 100000 AS "Casos por 100 mil hab."
FROM source_data_cumulative_sum
WHERE 
    "cumulative_cases" > 0 AND
    location not in ('NOT REPORTED') AND
    state <> 'NOT REPORTED'
ORDER BY "semanas epidemiologicas", "state_code", "location"
    