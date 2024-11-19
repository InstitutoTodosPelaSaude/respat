{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "hpardini_raw") }}
)
SELECT 
    "CODIGO" AS test_id,
    TO_DATE("DATACOLETA", 'YYYY-MM-DD') AS date_testing,
    {{ normalize_text("PATÓGENO") }} AS pathogen,
    {{ normalize_text("MÉTODO") }}   AS detalhe_exame,
    {{ normalize_text("CIDADE") }}   AS location,
    {{ normalize_text("UF") }}       AS state,
    "SEXO" AS sex,
    "IDADE"::BIGINT AS age,
    {{ normalize_text("RESULTADO") }} AS result,
    "file_name" 
FROM source_data