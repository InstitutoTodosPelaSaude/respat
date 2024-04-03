{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        *
    FROM
    {{ ref("combined_historical_01_fix_types") }}

)
SELECT
        sample_id,
        CASE lab_id
            WHEN 'DB Mol' THEN 'DBMOL'
            WHEN 'HLAGyn' THEN 'HLAGYN'
            ELSE lab_id
        END AS lab_id,
        test_id,
        test_kit,
        date_testing,
        state_code,
        patient_id,
        
        CASE 
            WHEN not sex in ('M', 'F') THEN NULL
            ELSE sex 
        END AS sex,
        
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
        {{ normalize_text("location") }} as location,
        {{ normalize_text("state") }} as state,
        'historical_combined' as file_name
FROM source_data
WHERE 1=1
AND date_testing < '{{ var('combined_threshold_date') }}' -- Apenas dados históricos
AND NOT ( 
    -- Filtrar linhas sem nenhum resultado de teste
    "SC2_test_result" = 'NT' AND
    "FLUA_test_result" = 'NT' AND
    "FLUB_test_result" = 'NT' AND
    "VSR_test_result" = 'NT' AND
    "RINO_test_result" = 'NT' AND
    "META_test_result" = 'NT' AND
    "PARA_test_result" = 'NT' AND
    "ADENO_test_result" = 'NT' AND
    "BOCA_test_result" = 'NT' AND
    "COVS_test_result" = 'NT' AND
    "ENTERO_test_result" = 'NT' AND
    "BAC_test_result" = 'NT'
)