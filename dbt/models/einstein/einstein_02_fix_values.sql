{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("einstein_01_convert_types") }}

)
SELECT
    *
FROM source_data