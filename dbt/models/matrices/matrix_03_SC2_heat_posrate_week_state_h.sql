{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("matrix_03_SC2_heat_posrate_week_state") }}
)

SELECT
    *
FROM source_data
WHERE 
    "Completude final" IN ('Apenas histórico', 'Alta qualidade')