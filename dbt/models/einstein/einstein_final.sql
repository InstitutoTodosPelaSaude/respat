{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ source("dagster", "einstein_raw") }}
)
SELECT * FROM source_table