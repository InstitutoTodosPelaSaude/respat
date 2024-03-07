{{ config(materialized='table') }}
{{ 
    dbt_utils.deduplicate(
        relation=ref('combined_historical_02_fix_values'),
        partition_by='sample_id',
        order_by='sample_id',
    )
}}