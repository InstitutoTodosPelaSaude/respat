{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("hpardini_02_fix_values") }}
)
SELECT 
    * 
FROM source_data