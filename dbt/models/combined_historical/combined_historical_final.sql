{{ config(materialized='table') }}

WITH source_data AS(
    SELECT
    *
    FROM {{ ref("combined_historical_03_deduplicate") }}
)
SELECT
    *,
    1 as qty_original_lines,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS updated_at,
    'COMBINED_HISTORICAL_DATA' as file_name
FROM source_data