{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "sabin_raw") }}

)
SELECT
    "OS" AS test_id,
    "Estado" AS state,
    "Municipio" AS location,
    TO_DATE("DataAtendimento", 'DD/MM/YYYY') AS date_testing, 
    TO_DATE("DataNascimento", 'DD/MM/YYYY') AS birth_date,  
    "Sexo" AS sex,
    "Descricao" AS exame,
    "Parametro" AS detalhe_exame,
    -- Normalize "Resultado"
    UNACCENT(UPPER(TRIM("Resultado"))) AS result,
    -- "DataAssinatura", 
    "file_name"
FROM source_data