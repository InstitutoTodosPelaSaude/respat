{{
    config(
        materialized='incremental',
        unique_key='sample_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['created_at'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_sivep_date_pri_sin ON {{ this.schema }}.{{ this.identifier }} (date_pri_sin)",
            "CREATE INDEX IF NOT EXISTS idx_sivep_state ON {{ this.schema }}.{{ this.identifier }} (state)",
            "CREATE INDEX IF NOT EXISTS idx_sivep_test_kit ON {{ this.schema }}.{{ this.identifier }} (test_kit)",
            "CREATE INDEX IF NOT EXISTS idx_sivep_age ON {{ this.schema }}.{{ this.identifier }} (age)"
        ]
    )
}}

WITH source_data AS(
    SELECT
    *
    FROM {{ ref("sivep_05_deduplicate") }}
)
SELECT
    *,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS updated_at
FROM source_data