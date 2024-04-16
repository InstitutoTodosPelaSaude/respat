{{ config(materialized='table') }}

{%
    set pathogen_list_that_indicates_positive_result = (
        'SINCICIAL RESPIRATORIO B',
        'HAEMOPHILUS INFLUENZAE',
        'CORONAVIRUS OC43',
        'INFLUENZA A',
        'PARAINFLUENZA 4',
        'METAPNEUMOVIRUS',
        'INFLUENZA A H1PDM09',
        'STREPTOCOCCUS PNEUMONIAE',
        'RINOVIRUS',
        'INFLUENZA A H3N2',
        'CHLAMYDOPHILA PNEUMONIAE',
        'PARAINFLUENZA 2',
        'ENTEROVIRUS',
        'INFLUENZA B',
        'CORONAVIRUS NL63',
        'CORONAVIRUS 229E',
        'ADENOVIRUS',
        'BOCAVIRUS',
        'PARAINFLUENZA 3',
        'MP',
        'MYCOPLASMA PNEUMONIAE',
        'SINCICIAL RESPIRATORIO A',
        'RSV A',
        'SP',
        'HI'
    )
%}

WITH source_table AS (
    SELECT * FROM
    {{ ref('sabin_01_convert_types') }}
)
SELECT
    md5(
        CONCAT(
            test_id,
            exame,
            CASE
                WHEN detalhe_exame IN ('RDRPALVO', 'NALVO') THEN 'NALVO_RDRPALVO'
                WHEN exame ILIKE 'PAINEL MOLECULAR RAPIDO PARA INFECÇOES RESPIRATORIAS COM SARSCOV2%'
                THEN 'test_21'
                WHEN exame ILIKE 'PAINEL MOLECULAR PARA INFECÇÕES RESPIRATÓRIAS%'
                THEN 'test_24'
                WHEN exame ILIKE 'PAINEL PARA VÍRUS SARS-CoV-2, INFLUENZA A, B E SINCICIAL RESPIRATÓ%'
                THEN 'test_4'
                ELSE detalhe_exame
            END
        )
    ) AS sample_id,
    test_id,
    state,
    location,
    date_testing,
    birth_date,
    
    CASE
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE NULL
    END AS sex,
    EXTRACT( YEAR FROM AGE(date_testing, birth_date) )::int AS age,

    exame,
    detalhe_exame,
    CASE
        WHEN result ILIKE 'NAO DETECTAD%' THEN 0
        WHEN result ILIKE 'DETECTAD%' THEN 1
        WHEN result IN {{ pathogen_list_that_indicates_positive_result }} THEN 1
        WHEN result = '0' THEN 0
        ELSE -2
    END AS result,
    
    CASE
        -- Testes da planilha de exames covid
        WHEN detalhe_exame IN ('RDRPALVO', 'NALVOSSA', 'NALVO', 'PCRSALIV', 'TMR19RES1') THEN 'covid_pcr'
        WHEN detalhe_exame IN ('COVIDECO') THEN 'covid_antigen'
        -- Testes da planilha de demais patógenos respiratórios
        WHEN detalhe_exame IN (
            'PARA1','PARA2', 'PARA3','PARA4',
            'BORDETELLAP','VSINCICIAL','CPNEUMONIAE',
            'ADEN','CORON','CORHKU','CORNL','CORC',
            'HUMANMET','HUMANRH','INFLUEH','INFLUEN','INFLUENZ','INFLUEB',
            'MYCOPAIN','PAINSARS','RSPAIN'
        ) 
        THEN 'test_21'
        WHEN detalhe_exame IN (
            'HPIV1', 'HPIV2', 'HPIV3', 'HPIV4',
            'RSVA', 'RSVB', 'MPVR', 'HRV', 
            'HBOV', 'HEVR', 'ADEV', 'BPP', 
            'BP', 'CP', 'MP', 'HI',
            'LP', 'SP', 'NL63', 'OC43', 'COR229E', 
            'H1N1R', 'H1PDM09', 'H3', 'INFLUA', 'INFLUB'
        ) 
        THEN 'test_24'
        WHEN detalhe_exame IN (
            'PCRVRESPBM', 'PCRVRESPBM2', 
            'PCRVRESPBM3', 'PCRVRESPBM4'
        ) 
        THEN 'test_4'
        ELSE 'UNKNOWN'
    END AS test_kit,

    file_name
FROM source_table
WHERE 1=1
-- SARS-COV2
-- Remove parameters 'RDRPCI', 'NALVOCI', 'NALVOCQ', 'NALVOCTL', 'RDRPALVOCTL'
-- These parameters are used in internal control
AND NOT detalhe_exame IN ('RDRPCI', 'NALVOCI', 'NALVOCQ', 'NALVOCTL', 'RDRPALVOCTL')
-- PCRESPSL
-- Remove parameters 'PCRESPSL' and 'PCRVRESP'
AND NOT detalhe_exame IN ('PCRESPSL', 'PCRVRESP')
-- PCRESPSL & PCRVRESP
-- These parametes are summarized by the 'PCRVRESPBM' parameter
AND NOT detalhe_exame IN ('GENES', 'GENERDRP', 'GENEN')
-- RESPIRA
-- Remove parameters RESPIRA1, RESPIRA2, RESPIRA3, RESPIRA4
AND NOT detalhe_exame IN ('RESPIRA', 'RESPIRA1', 'RESPIRA2', 'RESPIRA3', 'RESPIRA4')