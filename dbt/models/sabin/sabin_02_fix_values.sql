{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ ref('sabin_01_convert_types') }}
)
SELECT
    *
FROM source_table