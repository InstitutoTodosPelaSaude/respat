{{
    config(
        materialized='incremental',
        unique_key=['file_name', 'test_id'],
        cluster_by=['file_name'],
        incremental_strategy='merge',
        merge_exclude_columns = ['created_at']
    )
}}
WITH source_data AS(
    SELECT
        *
    FROM {{ ref("hlagyn_01_convert_types") }}
)
SELECT
    *,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS updated_at
FROM source_data