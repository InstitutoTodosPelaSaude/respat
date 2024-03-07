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
        'INFLUENZA A H3N2'
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
        WHEN detalhe_exame IN ('RDRPALVO', 'NALVOSSA', 'NALVO', 'PCRSALIV') THEN 'covid_pcr'
        WHEN detalhe_exame IN ('TMR19RES1') THEN 'thermo'
        WHEN detalhe_exame IN ('COVIDECO') THEN 'covid_antigen'
        -- Testes da planilha de demais patógenos respiratórios
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