{{ config(materialized='table') }}

{% set last_year_days_threshold = 364 %} -- 52 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_08_Resp_line_bar_posrate_posneg_week_country_h") }}
),

last_epiweek AS (
    SELECT MAX("semanas_epidemiologicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "semanas_epidemiologicas" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}  