{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "dbmol_raw") }}

)
SELECT
    "NumeroPedido" as test_id,
    TO_DATE("DataCadastro", 'YYYY-MM-DD') as date_testing,
    TO_DATE("DataNascimento", 'YYYY-MM-DD') as data_nascimento, 
    {{ normalize_text("CodigoProcedimento") }} as codigo_procedimento,
    {{ normalize_text("Parametro") }} as parametro,
    {{ normalize_text("Procedimento") }} as procedimento,
    UPPER("Resultado")::text as result,
    "Sexo" as sex,
    {{ normalize_text("Cidade") }} as location,
    "UF" as state_code,
    file_name
FROM source_data