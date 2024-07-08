{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        sample_id,
        test_kit,
        epiweek_enddate,
        lab_id,
        CASE WHEN location IS NULL THEN 'NOT REPORTED' ELSE location END AS location,
        location_ibge_code,
        CASE WHEN state IS NULL THEN 'NOT REPORTED' ELSE state END AS state,
        CASE WHEN state_code IS NULL THEN 'NOT REPORTED' ELSE state_code END AS state_code,
        CASE WHEN country IS NULL THEN 'NOT REPORTED' ELSE country END AS country,
        CASE WHEN age_group IS NULL THEN 'NOT REPORTED' ELSE age_group END AS age_group,
        lat,
        long,
        "SC2_test_result",
        "FLUA_test_result",
        "FLUB_test_result",
        "VSR_test_result",
        "COVS_test_result",
        "ADENO_test_result",
        "BOCA_test_result",
        "RINO_test_result",
        "PARA_test_result",
        "ENTERO_test_result",
        "META_test_result",
        "BAC_test_result"

    FROM {{ ref("combined_final") }}
    WHERE 
        epiweek_enddate < CURRENT_DATE AND
        epiweek_enddate >= '2022-01-01'
)
SELECT
    combined.*,
    combined_pivoted.*
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