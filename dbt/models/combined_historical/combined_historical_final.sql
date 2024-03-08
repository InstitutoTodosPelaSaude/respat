{{ config(materialized='table') }}

WITH source_data AS(
    SELECT
    *
    FROM {{ ref("combined_historical_03_deduplicate") }}
)
SELECT
    *
FROM source_data