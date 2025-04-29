{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "target_raw") }}

)

SELECT 
    "Numerdo do Pedido"::BIGINT::TEXT               AS test_id,
    "Código da Amostra"::BIGINT::TEXT               AS codigo_amostra,            
    TO_DATE("Data de Coleta", 'DD/MM/YYYY')         AS date_testing,
    "Sexo"                                          AS sex,
    {{ normalize_text("Idade") }}                   AS age,
    {{ normalize_text("Município") }}               AS location,
    {{ normalize_text("Estado") }}                  AS state_code,
    {{ normalize_text("Exame") }}                   AS exame,
    {{ normalize_text("Nome exame") }}              AS nome_exame,
    "Cód. Pátógeno"                                 AS detalhe_exame,
    {{ normalize_text("Patógeno") }}                AS pathogen,
    {{ normalize_text("Resultado") }}               AS result,
    file_name
FROM source_data
WHERE "Numerdo do Pedido"::BIGINT::TEXT NOT IN ('638745', '603268') -- Tests with future date