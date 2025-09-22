{{ config(materialized='table') }}


WITH source_data AS (
    SELECT * 
    FROM {{ ref('hlagyn_02a_pathogens__fix_values') }}
), results_grouped_by_pathogen AS (
    SELECT
        MAX(sample_id) AS sample_id,
        --test_id,
        MAX(date_testing) AS date_testing,
        MAX(age) AS age,
        MAX(sex) AS sex,
        MAX(location) AS location,
        MAX(state_code) AS state_code,
        MAX(state) AS state,

        MAX(original_test_name) AS original_test_name,
        MAX(original_test_detail_name) AS original_test_detail_name,
        
        MAX(test_kit) AS test_kit,
        MAX(test_name) AS test_name,
        BOOL_OR(is_a_multipathogen_test) AS is_a_multipathogen_test,
        MAX(test_methodology) AS test_methodology,

        --pathogen_name,
        --pathogen_detail,
        --pathogen_type,
        MAX(pathogen_result) AS pathogen_result,

        test_id, pathogen_name, pathogen_type, pathogen_detail
    FROM source_data
    GROUP BY test_id, pathogen_name, pathogen_type, pathogen_detail
)
SELECT
    *
FROM results_grouped_by_pathogen