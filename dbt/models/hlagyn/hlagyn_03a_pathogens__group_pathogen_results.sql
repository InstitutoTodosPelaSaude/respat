

{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * 
    FROM {{ ref('hlagyn_02a_pathogens__fix_values') }}
)
SELECT
*
FROM source_data