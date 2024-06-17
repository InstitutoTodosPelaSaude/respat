{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        result,
        pathogen
    FROM {{ ref("matrices_01_unpivot_combined") }}
)
SELECT
    epiweek_enddate,
    age_group,
    pathogen,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY epiweek_enddate, age_group, pathogen
ORDER BY epiweek_enddate, age_group, pathogen
