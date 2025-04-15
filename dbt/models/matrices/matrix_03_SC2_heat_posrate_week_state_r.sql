{{ config(materialized='table') }}

{% set last_year_days_threshold = 364 %} -- 52 weeks

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_03_SC2_heat_posrate_week_state") }}
),

last_epiweek AS (
    SELECT MAX("semanas_epidemiologicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "Completude final" IN ('Apenas 12 meses', 'Alta qualidade') AND
    "semanas_epidemiologicas" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}