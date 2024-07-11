{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        result
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        test_kit IN ('test_14', 'test_21', 'test_24', 'test_4',)
)
SELECT
    epiweek_enddate,
    pathogen,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY epiweek_enddate, pathogen
ORDER BY epiweek_enddate, pathogen
