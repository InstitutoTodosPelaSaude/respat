{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        *
    FROM
    {{ ref("sabin_02_fix_values") }}
)
SELECT
    *,
    {{ 
        pivot_pathogen_results(
            [
                'NALVOSSA',
                'NALVO',
                'TMR19RES1',
                'COVIDECO',
                'RDRPALVO',
                'PCRSALIV',

                'RDRPALVO', 
            
                
                'PAINSARS', 

                
                'PCRVRESPBM',  
            ], 
            'detalhe_exame', 
            'result', 
            'SC2_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                
                'INFLUEH', 
                'INFLUEN', 
                'INFLUENZ', 

                
                'INFLUA','H1N1R', 'H1PDM09', 'H3',

                
                'PCRVRESPBM2', 
            ], 
            'detalhe_exame', 
            'result', 
            'FLUA_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                
                'INFLUEB', 

                
                'INFLUB',

                
                'PCRVRESPBM3', 
            ], 
            'detalhe_exame', 
            'result', 
            'FLUB_test_result'
        )
    }}, 
    {{ 
        pivot_pathogen_results(
            [
                
                'VSINCICIAL',

                
                'RSVA', 'RSVB',

                
                'PCRVRESPBM4',
            ], 
            'detalhe_exame', 
            'result', 
            'VSR_test_result'
        )
    }},
    {{ 
        pivot_pathogen_results(
            [
                
                'HUMANMET',  

                
                'MPVR',
            ], 
            'detalhe_exame', 
            'result', 
            'META_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'HUMANRH',   
                
                
                'HRV',
            ], 
            'detalhe_exame', 
            'result', 
            'RINO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'PARA1','PARA2','PARA3','PARA4',

                
                'HPIV1', 'HPIV2', 'HPIV3', 'HPIV4'
            ], 
            'detalhe_exame', 
            'result', 
            'PARA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'ADEN', 

                
                'ADEV',
            ], 
            'detalhe_exame', 
            'result', 
            'ADENO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'HBOV',
            ], 
            'detalhe_exame', 
            'result', 
            'BOCA_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'CORON',       
                'CORHKU',      
                'CORNL',       
                'CORC',        

                
                'NL63', 'OC43', 'COR229E',
            ], 
            'detalhe_exame', 
            'result', 
            'COVS_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'HEVR',
            ], 
            'detalhe_exame', 
            'result', 
            'ENTERO_test_result'
        )
    }},
    {{
        pivot_pathogen_results(
            [
                
                'CPNEUMONIAE', 
                'MYCOPAIN',    
                'BORDETELLAP', 
                'RSPAIN',      

                
                'BPP',	
                'BP',	
                'CP',	
                'MP',	
                'HI',	
                'LP',	
                'SP',	
            ], 
            'detalhe_exame', 
            'result', 
            'BAC_test_result'
        )
    }}
FROM source_data