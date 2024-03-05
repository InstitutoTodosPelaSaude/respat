{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('fleury_03_explode_fluab_results'), except=["result"]) %}

WITH source_table AS (
    SELECT * FROM 
    {{ ref('fleury_03_explode_fluab_results') }}
)
SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    CASE result
        WHEN 'POSITIVO' THEN 1
        WHEN 'DETECTADO (POSITIVO)' THEN 1

        WHEN 'NEGATIVO' THEN 0
        WHEN 'NAO DETECTADO (NEGATIVO)' THEN 0

        WHEN 'INCONCLUSIVO' THEN 0

        ELSE -2
    END AS result
FROM source_table

    