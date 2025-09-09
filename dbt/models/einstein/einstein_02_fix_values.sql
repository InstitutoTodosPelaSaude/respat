{{ config(materialized='table') }}

WITH source_data AS (

    SELECT 
        
    md5(
        CONCAT(
            test_id,
            exame,
            CASE
                WHEN exame = 'INATIVO PMPR' THEN 'INATIVO PMPR'
                WHEN exame = 'PAINEL MOLECULAR PARA PNEUMONIA' THEN 'PAINEL MOLECULAR PARA PNEUMONIA'
                WHEN exame ILIKE 'PAINEL MOLECULAR PATOGENOS RESPIRATORIO%' THEN 'PAINEL MOLECULAR PATOGENOS RESPIRATORIOS'
                WHEN exame = 'PCR PAINEL DE PATOGENOS RESPIRATORIO' THEN 'PCR PAINEL DE PATOGENOS RESPIRATORIO'
                WHEN exame = 'PCR PARA INFLUENZA A/B E VRS' THEN 'PCR PARA INFLUENZA A/B E VRS'
                WHEN exame = 'PESQUISA RAPIDA PARA INFLUENZA A E B' THEN 'PESQUISA RAPIDA PARA INFLUENZA A E B'
                WHEN exame = 'PESQUISA RAPIDA PARA INFLUENZA A E B GL' THEN 'PESQUISA RAPIDA PARA INFLUENZA A E B GL'
                WHEN exame ILIKE 'TESTE MOLECULAR COVID-19, AMPLIFICACAO ISO%' THEN 'TESTE MOLECULAR COVID-19, AMPLIFICACAO ISOTERMICA'
                ELSE detalhe_exame
            END
        )
    ) AS sample_id,

    test_id,
    
    CASE
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE NULL
    END AS sex,

    CASE
        WHEN age > 120 OR age < 0 THEN NULL
        ELSE age
    END AS age,
    exame,
    
    REGEXP_REPLACE(
        REGEXP_REPLACE(detalhe_exame, '[.:]', '', 'g'), 
        '\s+', ' ', 'g'
    )
    AS detalhe_exame,

    date_testing,
    location,
    state,
    pathogen,
    
    CASE
        result
        WHEN 'DETECTADO' THEN 1
        WHEN 'NAO DETECTADO' THEN 0
        ELSE NULL
    END AS result,

    CASE exame
        WHEN '24 HRS COMPANHIA AEREA - PCR COVID19'              THEN 'covid_pcr'
        WHEN 'EXCLUSIVO EMPRESAS PCR COVID-19'                   THEN 'covid_pcr'
        WHEN 'HMSC - TESTE MOLECULAR ISOTERMICO'                 THEN 'covid_pcr'
        WHEN 'HMSC - TESTE MOLECULAR ISOTERMICO COVID-19'        THEN 'covid_pcr'
        WHEN 'INATIVO PMPR'                                      THEN 'test_4'
        WHEN 'OPERACAO AEROPORTO ANTIGENO COVID-19'              THEN 'covid_antigen'
        WHEN 'OPERACAO AEROPORTO PCR COVID-19'                   THEN 'covid_pcr'
        WHEN 'PAINEL MOLECULAR PARA PNEUMONIA'                   THEN 'test_3'
        WHEN 'PCR COVID19 EXPRESS'                               THEN 'covid_pcr'
        WHEN 'PCR EM TEMPO REAL PARA DETECCAO DE CORONAVIRUS'    THEN 'covid_pcr'
        WHEN 'PCR PAINEL DE PATOGENOS RESPIRATORIO'              THEN 'test_4'
        WHEN 'PCR PARA INFLUENZA A/B E VRS'                      THEN 'test_3'
        WHEN 'PESQ RAPIDA VIRUS SINCICIAL RESPIRATORIO'          THEN 'vsr_antigen'
        WHEN 'PESQUISA RAPIDA PARA INFLUENZA A E B'              THEN 'flu_antigen'
        WHEN 'PESQUISA RAPIDA PARA INFLUENZA A E B GL'           THEN 'flu_antigen'
        WHEN 'SALIVA, PCR PARA COVID-19'                         THEN 'covid_pcr'
        WHEN 'TESTE MOLECULAR COVID-19, AMPLIFICACAO ISOTERMICA' THEN 'covid_pcr'
        WHEN 'TESTE RAPIDO-ANTIGENO COVID-19 (SARS COV-2)'       THEN 'covid_antigen'
        WHEN 'TX PCR COVID19'                                    THEN 'covid_pcr'
        ELSE 
            CASE 
                WHEN exame ILIKE 'PAINEL MOLECULAR PATOGENOS RESPIRATORIOS%' THEN 'test_4'
                ELSE 'UNKNOWN'
            END
    END AS test_kit,

    file_name
    FROM
    {{ ref("einstein_01_convert_types") }}

),

-- This query selects data from the source table with specific filters to remove out-of-context cases (year < 2023 and age = 182)
rows_to_delete as (
    SELECT *
    FROM source_data
    WHERE REGEXP_SUBSTR(date_testing::TEXT, '\d{4}')::INT < 2023 
    AND age = 182
)

SELECT
    *
FROM source_data
WHERE 1=1
AND sample_id NOT IN (SELECT sample_id FROM rows_to_delete)
AND NOT (date_testing = '2024-05-10' AND age = 124)
AND NOT (exame ILIKE 'ZZ%')
AND date_testing >= '2025-08-01' -- Com as mudanças dos IDs do Einstein, consideramos dados a partir de 2025-08-01 para inclusão