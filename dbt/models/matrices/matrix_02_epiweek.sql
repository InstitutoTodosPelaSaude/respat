{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        result
    FROM {{ ref("matrices_01_unpivot_combined") }}
)
SELECT
    epiweek_enddate,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
