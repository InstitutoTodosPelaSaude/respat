{{ config(materialized='table') }}
{% set result_is_numeric = "result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]'" %}
{% set result_removido_termos_INFERIOR_SUPERIOR_A = "REPLACE(REPLACE(result, 'INFERIOR A ', ''), 'SUPERIOR A ', '')" %}
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
                    WHEN codigo_exame IN ('RESP4', 'PRESP', 'FLUAB', 'BORPC') -- Esses testes devem ser agrupados em uma única linha
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
            WHEN exame = 'ANTICORPOS IGM ANTI ADENOVIRUS'                         THEN 'adeno_igm'
            WHEN exame = 'ADENOVIRUS - PESQUISA'                                  THEN 'adeno_antigen'
            WHEN exame = 'ADENOVIRUS'                                             THEN 'adeno_pcr'
            WHEN exame = 'ANTICORPOS IGA ANTI ADENOVIRUS'                         THEN 'adeno_iga'

            WHEN exame = 'ANTICORPOS IGA ANTI MYCOPLASMA PNEUMONIAE'              THEN 'bac_iga'
            WHEN exame = 'ANTICORPOS IGG ANTI LEGIONELLA PNEUMOPHILA'             THEN 'bac_igg'
            WHEN exame = 'ANTIGENO DE STREPTOCOCCUS PNEUMONIAE'                   THEN 'bac_antigen'
            WHEN exame = 'ANTIGENO LEGIONELLA PNEUMOPHILA'                        THEN 'bac_antigen'
            WHEN exame = 'DETECCAO DE BORDETELLA PERTUSSIS E PARAPERTUSSIS'       THEN 'bac_pcr'
            WHEN exame = 'DETECCAO DE LEGIONELLA PNEUMOPHILA'                     THEN 'bac_pcr'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG E IGM'           THEN 'bac_antibodies'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG'                 THEN 'bac_igg'
            WHEN exame = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGM'                 THEN 'bac_igm'
            
            WHEN exame = 'INFLUENZA A VIRUS ANTICORPOS IGG'                       THEN 'flua_igg'
            WHEN exame = 'INFLUENZA A VIRUS ANTICORPOS IGM'                       THEN 'flua_igm'
            WHEN exame = 'INFLUENZA B VIRUS ANTICORPOS IGG'                       THEN 'flub_igg'
            WHEN exame = 'INFLUENZA B VIRUS ANTICORPOS IGM'                       THEN 'flub_igm'
            WHEN exame = 'INFLUENZA A E B - DETECCAO POR PCR'                     THEN 'flu_pcr'

            WHEN exame = 'PAINEL DE VIRUS RESPIRATORIO SARS-COV-2, VIRUS SINCICIAL, INFLUENZA A, INFLUENZA B' THEN 'test_4'
            WHEN exame = 'PAINEL MOLECULAR PARA DETECCAO DE VIRUS'                                            THEN 'test_11'
            WHEN exame = 'PAINEL RESPIRATORIO - PLUS (24 PATOGENOS INCLUINDO SARS COV-2)'                     THEN 'test_24'

            WHEN exame = 'ANTICORPOS IGG ANTI VIRUS SINCICIAL RESPIRATORIO (VSR)'                             THEN 'vsr_igg'
            WHEN exame = 'CORONAVIRUS 2019 - SARS-COV-2 IGG QUANTITATIVO'                                     THEN 'sc2_igg'
            WHEN exame = 'TESTE DE NEUTRALIZACAO SARS-COV-2/COVID19, ANTICORPOS TOTAIS - SORO'                THEN 'sc2_antigen'
            WHEN exame = 'CORONAVIRUS 2019 ANTICORPOS IGA (COVID19)'                                          THEN 'covid_antigen'
            
            ELSE 'UNKNOWN'
        END AS test_kit,
        codigo_exame,
        detalhe_exame,
        exame,

        CASE 
            WHEN codigo_exame IN (
                'RESP4', 
                'INFAM', 
                'INFBG', 'INFBM',
                'FLUAB', -- Influezna A e B
                'PRESP',  -- Test 24
                
                -- Adenovirus
                'ADENF', 'DNADE', 'ADENA', 'ADENM',
                -- VSR
                'SINRE',

                -- BACTERIA
                -- 'MYPNA','MYPNE',
                'LEGPG','LEGIO','LEGPN',
                'BORPC','ANSP'
            ) 
            THEN
                CASE
                    WHEN 
                        result IN ('NAO DETECTADO', 'NEGATIVO', 'NAO DETECTATDO') 
                        OR result ILIKE 'INFERIOR A%' 
                    THEN 0
                    WHEN 
                        result IN ('DETECTADO', 'POSITIVO') 
                        OR result ILIKE 'DETECTADO %' -- Para testes de Infleunza A e B
                    THEN 1
                    ELSE {{NAO_RECONHECIDO}}
                END 

            -- ADENO (ENZIMAIMUNOENSAIO)
            WHEN codigo_exame IN ('ADENO')
            THEN {{ map_result_values_to_negative_positive_and_undefined(
                    result_removido_termos_INFERIOR_SUPERIOR_A,
                    8.0, 10.0, 
                    INDETERMINADO, NAO_RECONHECIDO
            ) }}
            
            WHEN codigo_exame IN ('INFAG') AND result ILIKE 'INFERIOR A%'
            THEN 0

            WHEN codigo_exame IN ('INFAG') AND NOT (result ILIKE 'INFERIOR A%')
            THEN {{ map_result_values_to_negative_positive_and_undefined(
                    'result', 
                    0.8, 1.1, 
                    INDETERMINADO, NAO_RECONHECIDO
            ) }}
            
            WHEN codigo_exame IN ('COVI19Q')
            THEN {{ map_result_values_to_negative_and_positive(
                    result_removido_termos_INFERIOR_SUPERIOR_A, 
                    30.0, 
                    NAO_RECONHECIDO
            ) }}


            WHEN codigo_exame IN ('NEUCOV')
            THEN {{ map_result_values_to_negative_and_positive(
                    result_removido_termos_INFERIOR_SUPERIOR_A, 
                    30.0, 
                    NAO_RECONHECIDO
            ) }}


            WHEN codigo_exame IN ('COV19A')
            THEN {{ map_result_values_to_negative_positive_and_undefined(
                    result_removido_termos_INFERIOR_SUPERIOR_A,
                    0.8, 1.09, 
                    INDETERMINADO, NAO_RECONHECIDO
            ) }}

            WHEN codigo_exame IN ('MYPNG')
            THEN {{ map_result_values_to_negative_and_positive(
                    result_removido_termos_INFERIOR_SUPERIOR_A, 
                    10.0, 
                    NAO_RECONHECIDO
            ) }}

            WHEN codigo_exame IN ('MYPNM')
            THEN {{ map_result_values_to_negative_and_positive(
                    result_removido_termos_INFERIOR_SUPERIOR_A, 
                    10.0, 
                    NAO_RECONHECIDO
            ) }}

            ELSE {{NAO_RECONHECIDO}}
        END AS result,

        CASE sex
            WHEN 'F' THEN 'F'
            WHEN 'M' THEN 'M'
            ELSE NULL
        END AS sex,
        location,
        state_code,

        {{ map_state_code_to_state_name('state_code', 'NULL') }} AS state,

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
        'SINRE', 
        'INFBG', 
        'INFBM',
        'INFLUA',
        'INFLUB',
        -- 'HRSVAB',
        'SARS',
        'FLUAB', 
        'BORPC', -- PARAPE e PERTUS
        'LEGIO', 
        'LEGPG', 
        'LEGPN', 
        'PRESP', 
        'DNADE',
        --'PVIROC', 
        --'MYPNE', 
        --'MYPNA', 
        'ANSP', 
        'ADENA',
        'ADENM'

    )
    AND detalhe_exame NOT IN ('MAT', 'MATERIAL', 'METODO', 'SOROTI', 'TITU', 'TIT', 'TITULO', 'LEGPG')
    AND detalhe_exame IS NOT NULL
    AND result NOT IN ('INCONCLUSIVO', 'INDETERMINADO')
)
SELECT
    *
FROM source_data_fix_values
WHERE result != {{ INDETERMINADO }}