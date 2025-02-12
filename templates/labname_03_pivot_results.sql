{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        *
    FROM
    {{ ref("labname_02_fix_values") }}
)
SELECT
    *,
    {{ 
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'SC2_test_result'
        )
    }},
    {{ 
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'FLUA_test_result'
        )
    }},
    {{ 
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'FLUB_test_result'
        )
    }}, ----
    {{ 
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'VSR_test_result'
        )
    }},
    {{ 
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'META_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'RINO_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'PARA_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'ADENO_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'BOCA_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'COVS_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'ENTERO_test_result'
        )
    }},
    {{
        pivot__results(
            [
                ''
            ], 
            '', 
            'result', 
            'BAC_test_result'
        )
    }}
FROM source_data