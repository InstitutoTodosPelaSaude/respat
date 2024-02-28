{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "einstein_raw") }}

)
SELECT
    "accession" AS test_id,
    "sexo" AS sex,
    "idade"::INT AS age,
    "exame" AS exame,
    "detalhe_exame" AS detalhe_exame,
    TO_DATE("dh_coleta", 'DD/MM/YYYY') AS date_testing,
    "municipio" AS location,
    "estado" AS state,
    "patogeno" AS pathogen,
    "resultado" AS result,
    file_name
FROM source_data