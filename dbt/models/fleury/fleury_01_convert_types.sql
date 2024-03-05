{{ config(materialized='table') }}

WITH source_table AS (
    SELECT * FROM 
    {{ source("dagster", "fleury_raw") }}
)
SELECT
    "CODIGO REQUISICAO"                     as test_id,
    TO_DATE("DATA COLETA", 'DD/MM/YYYY')    as date_testing,
    "PACIENTE"                              as patient_id,
    "SEXO"                                  as sex,
    "IDADE"                                 as age,
    "MUNICIPIO"                             as location,
    "ESTADO"                                as state_code,
    {{ normalize_text("EXAME") }}           as exame,
    {{ normalize_text("PATOGENO") }}        as pathogen,
    {{ normalize_text("RESULTADO") }}       as result,
    file_name
FROM source_table
WHERE "EXAME" IS NOT NULL