{{ config(materialized='table') }}

{% set last_year_days_threshold = 56 %} -- 8 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_09_Resp_pyr_pos_agegroups_all_week_country_h") }}
),

last_epiweek AS (
    SELECT MAX("semana_epidemiológica") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "semana_epidemiológica" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}  