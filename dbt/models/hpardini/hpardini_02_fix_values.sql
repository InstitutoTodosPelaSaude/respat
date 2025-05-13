{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("hpardini_01_convert_types") }}
)
SELECT 
    md5(test_id) AS sample_id,
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

    date_testing,
    location,
    {{ map_state_code_to_state_name('state', 'NULL') }} state,
    pathogen,
    detalhe_exame,

    CASE
        result
        WHEN 'POSITIVO' THEN 1
        WHEN 'NEGATIVO' THEN 0
        ELSE NULL
    END AS result,

    CASE
        WHEN pathogen = 'SARS-COV-2' AND detalhe_exame = 'PCR EM TEMPO REAL'
        THEN 'covid_pcr'
        ELSE 'UNKNOWN'
    END AS test_kit,
    file_name

FROM source_data
WHERE pathogen NOT IN ('DENV', 'CHIKV', 'INFLUENZA B', 'INFLUENZA A', 'RSV')