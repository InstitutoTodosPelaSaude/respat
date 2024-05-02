{{ config(materialized='table') }}
{% set result_is_numeric = "result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]'" %}
{% set INDETERMINADO   = -3 %}
{% set NAO_RECONHECIDO = -2 %}

WITH source_data AS (
    SELECT *
    FROM {{ ref("dbmol_01_convert_types") }}
),
source_data_and_method AS (
    SELECT
        *
        --,MAX(
        --     CASE detalhe_exame
        --         WHEN 'METODO' THEN result
        --         ELSE NULL
        --     END 
        -- ) OVER (PARTITION BY test_id, exame) AS method
    FROM source_data
),
source_data_fix_values AS (
    SELECT
        MD5(
            CONCAT(
                test_id,
                exame,
                CASE 
                    WHEN codigo_exame IN ('RESP4') 
                    THEN codigo_exame
                    ELSE detalhe_exame
                END
            )
        ) AS sample_id,
        test_id,
        date_testing,
        CASE
            WHEN EXTRACT( YEAR FROM AGE(date_testing, data_nascimento) )::int > 120
            THEN NULL
            ELSE EXTRACT( YEAR FROM AGE(date_testing, data_nascimento) )::int
        END AS age,
        CASE 
            WHEN exame = 'ADENOVIRUS - ANTICORPOS IGG'                            THEN 'adeno_igg'
            WHEN exame = 'ADENOVIRUS - PESQUISA'                                  THEN 'adeno_test'
            WHEN exame = 'ADENOVIRUS'                                             THEN 'adeno_test'
            WHEN exame = 'ANTICORPOS IGA ANTI ADENOVIRUS'                         THEN 'bac_antigen'
            WHEN exame = 'ANTICORPOS IGA ANTI MYCOPLASMA PNEUMONIAE'              THEN 'bac_antigen'
            WHEN exame = 'ANTICORPOS IGG ANTI LEGIONELLA PNEUMOPHILA'             THEN 'bac_igg'
            WHEN exame = 'ANTICORPOS IGG ANTI VIRUS SINCICIAL RESPIRATORIO (VSR)' THEN 'vsr_igg'
            WHEN exame = 'ANTIGENO DE STREPTOCOCCUS PNEUMONIAE'                   THEN 'bac_antigen'
            WHEN exame = 'ANTIGENO LEGIONELLA PNEUMOPHILA'                        THEN 'bac_antigen'
            WHEN exame = 'CORONAVIRUS 2019 - SARS-COV-2 IGG QUANTITATIVO'         THEN 'sc2_igg'
            WHEN exame = 'DETECCAO DE BORDETELLA PERTUSSIS E PARAPERTUSSIS'       THEN 'bac_test'
            WHEN exame = 'DETECCAO DE LEGIONELLA PNEUMOPHILA'                     THEN 'bac_test'
            WHEN exame = 'INFLUENZA A E B - DETECCAO POR PCR'                     THEN 'test_2'
            WHEN exame = 'INFLUENZA A VIRUS ANTICORPOS IGG'                       THEN 'flua_igg'
            WHEN exame = 'INFLUENZA A VIRUS ANTICORPOS IGM'                       THEN 'flua_igm'
            WHEN exame = 'INFLUENZA B VIRUS ANTICORPOS IGG'                       THEN 'flub_igg'
            WHEN exame = 'INFLUENZA B VIRUS ANTICORPOS IGM'                       THEN 'flub_igm'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG E IGM'           THEN 'bac_test_2'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG'                 THEN 'bac_igg'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGM'                 THEN 'bac_igm'
            WHEN exame = 'PAINEL DE VIRUS RESPIRATORIO SARS-COV-2, VIRUS SINCICIAL, INFLUENZA A, INFLUENZA B' 
                                                                                THEN 'test_4'
            WHEN exame = 'PAINEL MOLECULAR PARA DETECCAO DE VIRUS'                THEN 'test_11'
            WHEN exame = 'PAINEL RESPIRATORIO - PLUS (24 PATOGENOS INCLUINDO SARS COV-2)' 
                                                                                THEN 'test_24'
            WHEN exame = 'TESTE DE NEUTRALIZACAO SARS-COV-2/COVID19, ANTICORPOS TOTAIS - SORO' 
                                                                                THEN 'sc2_antigen'
            ELSE 'UNKNOWN'
        END AS test_kit,
        codigo_exame,
        detalhe_exame,
        exame,

        CASE 
            WHEN codigo_exame IN ('RESP4', 'INFAM') 
            THEN
                CASE
                    WHEN result IN ('NAO DETECTADO', 'NEGATIVO') THEN 0
                    WHEN result IN ('DETECTADO',     'POSITIVO') THEN 1
                    ELSE {{INDETERMINADO}}
                END 

            -- ADENO (ENZIMAIMUNOENSAIO)
            WHEN codigo_exame IN ('ADENO')
            THEN {{ map_result_values_to_negative_positive_and_undefined(8.0, 10.0, INDETERMINADO, NAO_RECONHECIDO) }}
            
            WHEN codigo_exame IN ('INFAG')
            THEN {{ map_result_values_to_negative_positive_and_undefined(0.8, 1.1, INDETERMINADO, NAO_RECONHECIDO) }}
            
            WHEN codigo_exame IN ('COVI19Q')
            -- replace 'INFERIOR A 23' with 23 in result
            THEN {{ map_result_values_to_negative_and_positive("REPLACE(result, 'INFERIOR A ', '')", 30.0, NAO_RECONHECIDO) }}



            ELSE {{INDETERMINADO}}
        END AS result,

        CASE sex
            WHEN 'F' THEN 'F'
            WHEN 'M' THEN 'M'
            ELSE NULL
        END AS sex,
        location,
        state_code,
        file_name
    FROM source_data_and_method
    WHERE 1=1
    AND codigo_exame IN
    (
        'MYPNG', 
        'MYPNM', 
        'RESP4', 
        'ADENO', 
        'INFAM', 
        'INFAG', 
        'COVI19Q',
        'NEUCOV', 
        'COV19A', 
        'ADENF', 
        'MYPNE', 
        'SINRE', 
        'INFBG', 
        'INFBM',
        'INFLUA',
        'INFLUB',
        'HRSVAB',
        'SARS',
        'FLUAB', 
        'BORPC', 
        'LEGIO', 
        'LEGPG', 
        'LEGPN', 
        'PRESP', 
        'DNADE',
        'PVIROC', 
        'MYPNA', 
        'ANSP', 
        'ADENA',
        'ADENM',

        -- PAINEL RESPIRATÓRIO - PLUS (24 PATÓGENOS INCLUINDO SARS COV-2)
        'ADENO',
        'BOCAV',
        'BPARAP',
        'BPERTU',
        'COR0C4',
        'COR229',
        'CORHK',
        'CORNL',
        'ENTERO',
        'INFLH3',
        'INFLU1',
        'INFLUA',
        'INFLUB',
        'METAP',
        'MYCOP',
        'PARA1',
        'PARA2',
        'PARA3',
        'PARA4',
        'RINOV',
        'SARSC',
        'SINCIA',
        'SINCIB'
    )
    AND detalhe_exame NOT IN ('MAT', 'MATERIAL', 'METODO', 'SOROTI', 'TITU', 'TIT', 'TITULO', 'LEGPG')
    AND detalhe_exame IS NOT NULL
    AND result NOT IN ('INCONCLUSIVO', 'INDETERMINADO')
)
SELECT
    *
FROM source_data_fix_values
WHERE result != {{ INDETERMINADO }}