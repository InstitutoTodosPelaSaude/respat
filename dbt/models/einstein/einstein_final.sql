{{
    config(
        materialized='incremental',
        unique_key='sample_id',
        incremental_strategy='merge',
        merge_exclude_columns = ['created_at']
    )
}}

{% set test_result_columns = ["SC2_test_result", "FLUA_test_result", "FLUB_test_result", "VSR_test_result", "META_test_result", "RINO_test_result", "PARA_test_result", "ADENO_test_result", "BOCA_test_result", "COVS_test_result", "ENTERO_test_result", "BAC_test_result"] %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('einstein_05_deduplicate'), except=["SC2_test_result", "FLUA_test_result", "FLUB_test_result", "VSR_test_result", "META_test_result", "RINO_test_result", "PARA_test_result", "ADENO_test_result", "BOCA_test_result", "COVS_test_result", "ENTERO_test_result", "BAC_test_result"]) %}

/*
    This query incrementally merges new test results into the existing table. 
For columns like SC2_test_result, FLUB_test_result, and FLUA_test_result, updates
occur only if the current value is 'NT'. This ensures that valid existing results are 
not overwritten, while still capturing new data where applicable.
*/

SELECT
    -- COLUMNS FROM SOURCE DATA
    {% for column in column_names %}
        source_data."{{ column }}",
    {% endfor %}

    -- TEST RESULT COLUMNS (IF TARGET IS NT, USE SOURCE DATA)
    {% for column in test_result_columns %}
        CASE
            WHEN target."{{ column }}" = 'NT' THEN source_data."{{ column }}"
            ELSE target."{{ column }}"
        END AS "{{ column }}",
    {% endfor %}

    -- EXTRA COLUMNS 
    NULL as patient_id,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS updated_at

FROM {{ ref("einstein_05_deduplicate") }} source_data
LEFT JOIN {{ this }} target
ON source_data.sample_id = target.sample_id