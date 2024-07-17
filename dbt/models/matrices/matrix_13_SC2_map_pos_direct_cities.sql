{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        state_code,
        location,
        location_ibge_code,
        lat,
        long,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        test_kit NOT IN ('sc2_igg') AND
        epiweek_enddate >= '2024-05-19'
    GROUP BY epiweek_enddate, state_code, location, location_ibge_code, lat, long, pathogen
    ORDER BY epiweek_enddate, state_code
),
source_data_sum AS (
    SELECT
        epiweek_enddate as "semanas epidemiologicas",
        location_ibge_code as "location_ibge_code",
        location as "location",
        state_code as "state",
        lat as "lat",
        long as "long",
        SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) as "cases"
    FROM source_data
    WHERE location IS NOT NULL
    GROUP BY epiweek_enddate, state_code, location_ibge_code, location, lat, long
),
source_data_cumulative_sum AS (
    SELECT
        "semanas epidemiologicas",
        "location_ibge_code",
        "location",
        "state",
        "lat",
        "long",
        SUM("cases") OVER (PARTITION BY "location_ibge_code" ORDER BY "semanas epidemiologicas") as "cases"
    FROM source_data_sum
    ORDER BY "semanas epidemiologicas", "state", "location"
)
SELECT
    "semanas epidemiologicas",
    "location_ibge_code",
    "location",
    "state",
    "lat",
    "long",
    "cases"
FROM source_data_cumulative_sum
WHERE "cases" > 0