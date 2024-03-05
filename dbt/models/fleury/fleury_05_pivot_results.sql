{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ ref('fleury_04_fix_result_values') }}
)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                "COVID 19, DETECCAO POR PCR",
                "COVID 19, ANTIGENO, TESTE RAPIDO",
                "COVIDFLURSVGX - SARS-COV-2",
                "COVID 19, DETECCAO POR PCR",
                "VIRUSMOL, SARS-COV-2"
            ], 
            'pathogen', 
            'result', 
            'SC2_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                "INFLUENZA A E B - TESTE RAPIDO FLU_A_RESULT",
                "COVIDFLURSVGX - INFLUENZA A",
                "VIRUS INFLUENZA A (SAZONAL)",
                "VIRUSMOL, INFLUENZA A",
                "VIRUSMOL, INFLUENZA A/H1",
                "VIRUSMOL, INFLUENZA A/H1-2009",
                "VIRUSMOL, INFLUENZA A/H3"
            ], 
            'pathogen', 
            'result', 
            'FLUA_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                "INFLUENZA A E B - TESTE RAPIDO FLU_B_RESULT",
                "COVIDFLURSVGX - INFLUENZA B",
                "VIRUSMOL, INFLUENZA B"
            ], 
            'pathogen', 
            'result', 
            'FLUB_test_result'
        )
    }}, ----
    {{ 
        pivot_pathogen_results(
            [
                "VIRUS RESPIRATORIO - SINCICIAL",
                "COVIDFLURSVGX - VIRUS SINCICIAL RESPIRATORIO",
                "VIRUSMOL, VIRUS SINCICIAL RESPIRATORIO",
                "VIRUS SINCIAL RESPIRATORIO"
            ], 
            'pathogen', 
            'result', 
            'VSR_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                "VIRUSMOL, METAPNEUMOVIRUS HUMANO"
            ], 
            'pathogen', 
            'result', 
            'META_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                "VIRUSMOL, RINOVIRUS/ENTEROVIRUS"
            ], 
            'pathogen', 
            'result', 
            'RINO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                "VIRUSMOL, PARAINFLUENZA 1",
                "VIRUSMOL, PARAINFLUENZA 2",
                "VIRUSMOL, PARAINFLUENZA 3",
                "VIRUSMOL, PARAINFLUENZA 4"
            ], 
            'pathogen', 
            'result', 
            'PARA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                "VIRUSMOL, ADENOVIRUS",
            ], 
            'pathogen', 
            'result', 
            'ADENO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                ''
            ], 
            'pathogen', 
            'result', 
            'BOCA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                "VIRUSMOL, CORONAVIRUS 229E",
                "VIRUSMOL, CORONAVIRUS HKU1",
                "VIRUSMOL, CORONAVIRUS NL63",
                "VIRUSMOL, CORONAVIRUS OC43"
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
                "VIRUSMOL, BORDETELLA PARAPERTUSSIS",
                "VIRUSMOL, BORDETELLA PERTUSSIS",
                "VIRUSMOL, CHLAMYDOPHILA PNEUMONIAE",
                "VIRUSMOL, MYCOPLASMA PNEUMONIAE"
            ], 
            'pathogen', 
            'result', 
            'BAC_test_result'
        )
    }}
FROM source_table
