

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
    WHERE 
        date_testing < '{{ var('combined_threshold_date') }}' AND
        lab_id NOT IN ('DASA')

    UNION

    SELECT
    lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("combined_historical_final") }}
    WHERE
        lab_id IN ('DASA')

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

    UNION

    SELECT
    'DBMOL' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("dbmol_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'

    UNION

    SELECT
    'TARGET' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("target_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'

    UNION

    SELECT
    'HPARDINI' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("hpardini_final") }}
    WHERE date_testing >= '{{ var('combined_threshold_date') }}'
)
SELECT
    source_data.*,
    methods.test_method as test_method
FROM source_data
LEFT JOIN {{ ref('test_methods') }} as methods ON source_data.test_kit = methods.test_kit