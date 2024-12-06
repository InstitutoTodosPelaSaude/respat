{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("labname_01_convert_types") }}
)
SELECT * FROM source_data