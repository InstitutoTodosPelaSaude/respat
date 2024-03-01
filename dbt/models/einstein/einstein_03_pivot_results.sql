{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        *
    FROM
    {{ ref("einstein_02_fix_values") }}
)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                "NEAR COVID-19",
                "RESULTADO COVID-19",
                "RESULTADO NEAR COVID-19",
                "SARS-COV-2 PAINEL",
                "TESTE RAPIDO ANTIGENO COVID-19"
            ], 
            'detalhe_exame', 
            'result', 
            'SC2_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                "INF A H1N1 2009",
                "INFLUENZA A",
                "INFLUENZA A, TESTE RAPIDO"
            ], 
            'detalhe_exame', 
            'result', 
            'FLUA_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                "INFLUENZA B",
                "INFLUENZA B, TESTE RAPIDO"
            ], 
            'detalhe_exame', 
            'result', 
            'FLUB_test_result'
        )
    }}, ----
    {{ 
        pivot_pathogen_results(
            [
                "VIRUS SINCICIAL RESPIRATORIO",
                "VRS, TESTE RAPIDO"
            ], 
            'detalhe_exame', 
            'result', 
            'VSR_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'META_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'RINO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'PARA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'ADENO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'BOCA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'COVS_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'ENTERO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'detalhe_exame', 
            'result', 
            'BAC_test_result'
        )
    }}
FROM source_data