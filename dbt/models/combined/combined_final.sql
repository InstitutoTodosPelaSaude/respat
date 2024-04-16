{{ config(materialized='view') }}

WITH source_data AS(
    SELECT
    *
    FROM {{ ref("combined_05_location") }}
    WHERE date_testing >= '{{ var('combined_start_date') }}'
)
SELECT
    *
FROM source_data