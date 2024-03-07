{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        'HISTORICAL_'||sample_id AS sample_id,
        lab_id,
        test_id,
        test_kit,
        --country,
        --region,
        TO_DATE("date_testing", 'YYYY-MM-DD') AS date_testing,
        state_code,
        -- epiweek,
        patient_id,
        sex,
        age::INT,
        --age_group,	
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
        -- Ct_FluB,
        -- Ct_geneN,
        -- Ct_FluA,
        -- Ct_RDRP,
        -- Ct_geneE,
        -- Ct_geneS,
        -- Ct_VSR,
        -- Ct_ORF1ab,
        -- geneS_detection,
        location,
        state
        -- ADM2_PT,
        -- ADM2_PCODE,
        -- lat,
        -- long,
    FROM
    {{ source("dagster", "combined_historical_raw") }}

)
SELECT
    *
FROM source_data