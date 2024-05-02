{{ config(materialized='view') }}

WITH source_data AS(
    SELECT * 
    FROM {{ ref("report_epirio_02_group_values") }}
)
SELECT
    *
FROM
    source_data