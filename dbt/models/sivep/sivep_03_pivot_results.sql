{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        *
    FROM
    {{ ref("sivep_02_fix_values") }}
)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'covid'
            ], 
            'pathogen', 
            'result', 
            'SC2_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                'flua'
            ], 
            'pathogen', 
            'result', 
            'FLUA_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                'flub'
            ], 
            'pathogen', 
            'result', 
            'FLUB_test_result'
        )
    }}, ----
    {{ 
        pivot_pathogen_results(
            [
                'vsr'
            ], 
            'pathogen', 
            'result', 
            'VSR_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                'meta'
            ], 
            'pathogen', 
            'result', 
            'META_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'rino'
            ], 
            'pathogen', 
            'result', 
            'RINO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'para'
            ], 
            'pathogen', 
            'result', 
            'PARA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'adeno'
            ], 
            'pathogen', 
            'result', 
            'ADENO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                'boca'
            ], 
            'pathogen', 
            'result', 
            'BOCA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'COVS_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'ENTERO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'BAC_test_result'
        )
    }}
FROM source_data