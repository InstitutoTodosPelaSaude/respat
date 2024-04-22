{{ config(materialized='view') }}

WITH source_data AS(
    SELECT * 
    FROM {{ ref("external_reports_rio_02_group_values") }}
)
SELECT
    *
FROM
    source_data