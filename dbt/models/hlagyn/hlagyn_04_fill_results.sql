{{ config(materialized='table') }}

{%set test_result_columns = [
        'SC2_test_result',
        'FLUA_test_result',
        'FLUB_test_result',
        'VSR_test_result',
        'META_test_result',
        'RINO_test_result',
        'PARA_test_result',
        'ADENO_test_result',
        'BOCA_test_result',
        'COVS_test_result',
        'ENTERO_test_result',
        'BAC_test_result'
    ]
%}

WITH source_data AS (

    SELECT * FROM
    {{ ref('hlagyn_03_group_pathogens') }}

)
SELECT
    sample_id,
    test_id,
    date_testing,
    age,
    sex,
    -- detalhe_exame,
    location,
    -- state_code,
    state,
    test_kit,
    NULL as patient_id,
    file_name,

    -- Mapping test results
    --  1: Pos if AT LEAST ONE of the results is positive
    --  0: Neg if there is no POS results and there is at least one NEG result
    -- -1: NT if there is no POS or NEG results
    {% for pathogen in test_result_columns %}
        CASE 
            WHEN MAX("{{pathogen}}") OVER( PARTITION BY sample_id) = 1 THEN 'Pos'
            WHEN MAX("{{pathogen}}") OVER( PARTITION BY sample_id) = 0 THEN 'Neg'
            WHEN MAX("{{pathogen}}") OVER( PARTITION BY sample_id) = -1 THEN 'NT'
        END AS "{{pathogen}}"
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    ,
    -- Count the number of lines in each sample_id
    1 AS qty_original_lines
FROM source_data