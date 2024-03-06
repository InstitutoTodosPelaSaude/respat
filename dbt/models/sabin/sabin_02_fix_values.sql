{{ config(materialized='table') }}

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
    sex,
    exame,
    detalhe_exame,
    result,
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