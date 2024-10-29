{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "labname_raw") }}
)
SELECT 1 AS example