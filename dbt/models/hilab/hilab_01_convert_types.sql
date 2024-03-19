{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "hilab_raw") }}
)
SELECT
    "Código Da Cápsula"                         AS test_id,
    {{ normalize_text("Estado") }}              AS state_code,
    {{ normalize_text("Cidade") }}              AS location,
    TO_DATE("Data Do Exame", 'DD/MM/YYYY')      AS date_testing,
    {{ normalize_text("Exame") }}               AS exame,
    {{ normalize_text("Resultado") }}           AS result,
    "Idade"::INT                                AS age,
    "Paciente"                                  AS patient_id,
    {{ normalize_text("Sexo") }}                AS sex,
    file_name
FROM source_data
WHERE 
    "Exame" NOT IN ('Zika IgG', 'Zika IgM', 'Dengue IgG', 'Dengue IgM', 'Dengue NS1') AND
    "Resultado" NOT IN ('Inválido')
