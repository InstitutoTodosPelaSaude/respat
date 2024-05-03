{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "dbmol_raw") }}

)
SELECT
    "NumeroPedido" as test_id,
    "Sexo" as sex,
    {{ normalize_text("Procedimento") }}       AS exame,
    {{ normalize_text("CodigoProcedimento") }} AS codigo_exame,
    {{ normalize_text("Parametro") }}          AS detalhe_exame,
    TO_DATE("DataCadastro", 'YYYY-MM-DD')      AS date_testing,
    TO_DATE("DataNascimento", 'YYYY-MM-DD')    AS data_nascimento, 
    {{ normalize_text("Cidade") }}             AS location,
    {{ normalize_text("UF") }}                 AS state_code,
    {{ normalize_text("Resultado") }}::text    AS result,
    file_name
FROM source_data