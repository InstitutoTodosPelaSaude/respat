{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ ref('fleury_01_convert_types') }}
)
SELECT
    md5(
        CONCAT(
            test_id,
            exame,
            pathogen
        )
    ) AS sample_id,
    test_id,
    date_testing,
    patient_id,
    sex,
    CASE
        WHEN regexp_like(age, '^[0-9]*$') THEN CAST(age AS INT)                     -- Examples: '100', '95'
        WHEN regexp_like(age, '^[0-9]*A') THEN CAST(SPLIT_PART(age, 'A', 1) AS INT) -- Examples: '100A', '95A10M'
        WHEN regexp_like(age, '^[0-9]*M') THEN 0                                    -- Examples: '11M', '11A10D'
        WHEN regexp_like(age, '^[0-9]*D') THEN 0                                    -- Examples: '11D', '30D'
        WHEN age IS NULL THEN NULL                                                  -- Examples: NULL
        ELSE -1                                                                     -- Avoid missing new formats
    END AS age,
    location,
    state_code,
    exame,
    CASE exame
        WHEN 'COVIDFLURSVGX'    THEN 'test_4'
        WHEN 'VIRUSMOL'         THEN 'test_21'
        WHEN '2019NCOV'         THEN 'covid_pcr'
        WHEN 'AGCOVIDNS'        THEN 'covid_antigen'
        WHEN 'AGINFLU'          THEN 'flu_antigen'
        WHEN 'AGSINCURG'        THEN 'vsr_antigen'
        WHEN 'COVID19GX'        THEN 'covid_pcr'
        WHEN 'COVID19SALI'      THEN 'covid_pcr'
        WHEN 'INFLUENZAPCR'     THEN 'flu_pcr'
        WHEN 'VRSAG'            THEN 'vsr_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    pathogen,
    result,
    file_name
FROM source_table