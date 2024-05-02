{{ config(materialized='table') }}

WITH source_data AS(
    SELECT * 
    FROM {{ ref("combined_for_reports") }}
    WHERE location_ibge_code = 3304557
)

SELECT
    combined.date_testing,
    combined.test_kit,
    combined.age_group,
    combined.location,
    combined.state_code,
    combined.location_ibge_code,
    combined_pivoted.pathogen,
    combined_pivoted.result
FROM
    source_data combined
CROSS JOIN LATERAL (
    VALUES
        (combined."SC2_test_result",    'SC2'),
        (combined."FLUA_test_result",   'FLUA'),
        (combined."FLUB_test_result",   'FLUB'),
        (combined."VSR_test_result",    'VSR'),
        (combined."COVS_test_result",   'COVS'),
        (combined."ADENO_test_result",  'ADENO'),
        (combined."BOCA_test_result",   'BOCA'),
        (combined."RINO_test_result",   'RINO'),
        (combined."PARA_test_result",   'PARA'),
        (combined."ENTERO_test_result", 'ENTERO'),
        (combined."META_test_result",   'META'),
        (combined."BAC_test_result",    'BAC')
) AS combined_pivoted(result, pathogen)
WHERE combined_pivoted.result <> 'NT'