{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('fleury_02_fix_values'), except=["pathogen", "result"]) %}

WITH 
others_source_table AS (
    SELECT * FROM {{ ref('fleury_02_fix_values') }}
    WHERE exame != 'AGINFLU' AND pathogen != 'INFLUENZA A E B - TESTE RAPIDO'
),
flu_a_source_table AS (
    SELECT * FROM {{ ref('fleury_02_fix_values') }}
    WHERE exame = 'AGINFLU' AND pathogen = 'INFLUENZA A E B - TESTE RAPIDO'
),
flu_b_source_table AS (
    SELECT * FROM {{ ref('fleury_02_fix_values') }}
    WHERE exame = 'AGINFLU' AND pathogen = 'INFLUENZA A E B - TESTE RAPIDO'
)

SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    pathogen,
    result
FROM others_source_table

UNION

SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    'INFLUENZA A E B - TESTE RAPIDO FLU_A_RESULT' AS pathogen,
    CASE
        WHEN result = 'NEGATIVO' THEN 'NEGATIVO'
        WHEN result = 'INFLUENZA A - POSITIVO' THEN 'POSITIVO'
        WHEN result = 'INFLUENZA B - POSITIVO' THEN 'NEGATIVO'
        WHEN result = 'INFLUENZA A E B - POSITIVO' THEN 'POSITIVO'
        ELSE 'UNKNOWN'
    END AS result
FROM flu_a_source_table

UNION

SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    'INFLUENZA A E B - TESTE RAPIDO FLU_B_RESULT' AS pathogen,
    CASE
        WHEN result = 'NEGATIVO' THEN 'NEGATIVO'
        WHEN result = 'INFLUENZA A - POSITIVO' THEN 'NEGATIVO'
        WHEN result = 'INFLUENZA B - POSITIVO' THEN 'POSITIVO'
        WHEN result = 'INFLUENZA A E B - POSITIVO' THEN 'POSITIVO'
        ELSE 'UNKNOWN'
    END AS result
FROM flu_b_source_table
