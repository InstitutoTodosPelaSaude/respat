{{ config(materialized='table') }}

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_16_VSR_map_pos_direct_states_h") }}
),

last_epiweek AS (
    SELECT MAX("Semanas epidemiológicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "Semanas epidemiológicas" >= (SELECT epiweek_enddate FROM last_epiweek)