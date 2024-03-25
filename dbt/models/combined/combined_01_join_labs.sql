

{{ config(materialized='table') }}

{%
    set columns = [
        'sample_id',
        'test_id',
        'test_kit',
        'sex',
        'age',
        'location',
        'date_testing',
        'state',
        'patient_id',
        'file_name',
        '"SC2_test_result"',
        '"FLUA_test_result"',
        '"FLUB_test_result"',
        '"VSR_test_result"',
        '"RINO_test_result"',
        '"META_test_result"',
        '"PARA_test_result"',
        '"ADENO_test_result"',
        '"BOCA_test_result"',
        '"COVS_test_result"',
        '"ENTERO_test_result"',
        '"BAC_test_result"',
        'qty_original_lines',
        'created_at',
        'updated_at'
    ]
%}

WITH source_data AS (

    SELECT
    lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("combined_historical_final") }}
    WHERE date_testing < '{{ var('combined_threshold_date') }}'

    UNION

    SELECT 
    'EINSTEIN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("einstein_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'
    
    UNION

    SELECT 
    'FLEURY' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("fleury_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'
    
    UNION
    
    SELECT 
    'SABIN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("sabin_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'

    UNION

    SELECT 
    'HILAB' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("hilab_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'

    UNION
    
    SELECT
    'HLAGYN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("hlagyn_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'
    
)
SELECT
    *
FROM source_data