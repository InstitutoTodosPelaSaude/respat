{{ config(materialized='table') }}

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_25_FLUA_map_pos_direct_states_h") }}
),

last_epiweek AS (
    SELECT MAX("semanas epidemiologicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "semanas epidemiologicas" >= (SELECT epiweek_enddate FROM last_epiweek)