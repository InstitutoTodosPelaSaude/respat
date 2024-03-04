{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ source("dagster", "fleury_raw") }}
)
SELECT * FROM source_table