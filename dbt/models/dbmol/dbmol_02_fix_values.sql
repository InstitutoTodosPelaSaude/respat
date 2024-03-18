{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref("dbmol_01_convert_types") }}
)
SELECT
    MD5(
        CONCAT(
            test_id,
            procedimento,
            parametro
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
        WHEN procedimento = 'ADENOVIRUS - ANTICORPOS IGG' THEN 'adeno_igg'
        WHEN procedimento = 'ADENOVIRUS - PESQUISA' THEN 'adeno_test'
        WHEN procedimento = 'ADENOVIRUS' THEN 'adeno_test'
        WHEN procedimento = 'ANTICORPOS IGA ANTI ADENOVIRUS' THEN 'bac_antigen'
        WHEN procedimento = 'ANTICORPOS IGA ANTI MYCOPLASMA PNEUMONIAE' THEN 'bac_antigen'
        WHEN procedimento = 'ANTICORPOS IGG ANTI LEGIONELLA PNEUMOPHILA' THEN 'bac_igg'
        WHEN procedimento = 'ANTICORPOS IGG ANTI VIRUS SINCICIAL RESPIRATORIO (VSR)' THEN 'vsr_igg'
        WHEN procedimento = 'ANTIGENO DE STREPTOCOCCUS PNEUMONIAE' THEN 'bac_antigen'
        WHEN procedimento = 'ANTIGENO LEGIONELLA PNEUMOPHILA' THEN 'bac_antigen'
        WHEN procedimento = 'CORONAVIRUS 2019 - SARS-COV-2 IGG QUANTITATIVO' THEN 'sc2_igg'
        WHEN procedimento = 'DETECCAO DE BORDETELLA PERTUSSIS E PARAPERTUSSIS' THEN 'bac_test'
        WHEN procedimento = 'DETECCAO DE LEGIONELLA PNEUMOPHILA' THEN 'bac_test'
        WHEN procedimento = 'INFLUENZA A E B - DETECCAO POR PCR' THEN 'test_2'
        WHEN procedimento = 'INFLUENZA A VIRUS ANTICORPOS IGG' THEN 'flua_igg'
        WHEN procedimento = 'INFLUENZA A VIRUS ANTICORPOS IGM' THEN 'flua_igm'
        WHEN procedimento = 'INFLUENZA B VIRUS ANTICORPOS IGG' THEN 'flub_igg'
        WHEN procedimento = 'INFLUENZA B VIRUS ANTICORPOS IGM' THEN 'flub_igm'
        WHEN procedimento = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG E IGM' THEN 'bac_test_2'
        WHEN procedimento = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGG' THEN 'bac_igg'
        WHEN procedimento = 'MYCOPLASMA PNEUMONIAE - ANTICORPOS IGM' THEN 'bac_igm'
        WHEN procedimento = 'PAINEL DE VIRUS RESPIRATORIO SARS-COV-2, VIRUS SINCICIAL, INFLUENZA A, INFLUENZA B' THEN 'test_4'
        WHEN procedimento = 'PAINEL MOLECULAR PARA DETECCAO DE VIRUS' THEN 'test_11'
        WHEN procedimento = 'PAINEL RESPIRATORIO - PLUS (24 PATOGENOS INCLUINDO SARS COV-2)' THEN 'test_24'
        WHEN procedimento = 'TESTE DE NEUTRALIZACAO SARS-COV-2/COVID19, ANTICORPOS TOTAIS - SORO' THEN 'sc2_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    codigo_procedimento,
    parametro,
    procedimento,
    CASE 
        WHEN result = 'DETECTADO' THEN 1
        WHEN result = 'POSITIVO' THEN 1

        WHEN result = 'SUPERIOR A 180.0' THEN 1
        WHEN result = 'SUPERIOR A 200.0' THEN 1
        WHEN result = 'SUPERIOR A 27.0' THEN 1
        WHEN result = 'SUPERIOR A 1800.0' THEN 1

        WHEN result = 'NÃO DETECTADO' THEN 0
        WHEN result = 'NEGATIVO' THEN 0

        WHEN result = 'INFERIOR A 0.1' THEN 0
        WHEN result = 'INFERIOR A 1.0' THEN 0
        WHEN result = 'INFERIOR A 2' THEN 0
        WHEN result = 'INFERIOR A 23' THEN 0
        WHEN result = 'INFERIOR A 8.0' THEN 0

        WHEN result ~ '^[0-9]+[\.]*[0-9]*$' THEN
            CASE 
                WHEN result::float > 0.8 THEN 1
                ELSE 0
            END
        ELSE -2
    END AS result,
    result as result_test,
    CASE sex
        WHEN 'F' THEN 'F'
        WHEN 'M' THEN 'M'
        ELSE NULL
    END AS sex,
    location,
    state_code,
    file_name
FROM source_data
WHERE 1=1
AND codigo_procedimento IN
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
    'ADENA'
)
AND parametro NOT IN ('MAT', 'MATERIAL', 'METODO', 'SOROTI', 'TITU', 'TIT', 'TITULO', 'LEGPG')
AND parametro IS NOT NULL
AND result NOT IN ('INCONCLUSIVO', 'INDETERMINADO')