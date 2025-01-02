{{ config(materialized='table') }}
{{ 
    dbt_utils.deduplicate(
        relation=ref('sivep_04_fill_results'),
        partition_by='sample_id',
        order_by='sample_id',
    )
}}