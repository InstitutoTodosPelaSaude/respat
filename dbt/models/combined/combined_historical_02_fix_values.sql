{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        *
    FROM
    {{ ref("combined_historical_01_fix_types") }}

)
SELECT
        sample_id,
        lab_id,
        test_id,
        test_kit,
        date_testing,
        state_code,
        patient_id,
        sex,
        
        CASE 
            WHEN age < 0 THEN NULL
            ELSE age
        END AS age,

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
        "BAC_test_result",
        location,
        state
FROM source_data