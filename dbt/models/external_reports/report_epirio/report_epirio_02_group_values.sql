{{ config(materialized='table') }}

WITH source_data AS(
    SELECT * 
    FROM {{ ref("report_epirio_01_filter_and_pivot") }}
)
SELECT
    -- Create the CUBE
    date_testing,
    test_kit,
    age_group,
    'Rio de Janeiro' AS location,
    'RJ' AS state_code,
    location_ibge_code,
    pathogen,
    result,

    COUNT(*) AS quantity

FROM
    source_data
GROUP BY date_testing, test_kit, age_group, location_ibge_code, pathogen, result
ORDER BY date_testing, pathogen, test_kit, age_group, result