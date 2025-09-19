

{{ config(materialized='table') }}

{% set result_column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('hlagyn_01_convert_types'), except=["test_id", "date_testing", "age", "sex", "detalhe_exame", "location", "state_code", "file_name"]) %}

WITH source_data AS (
    SELECT * 
    FROM {{ ref('hlagyn_01_convert_types') }}
),
fix_values AS (
    SELECT
        md5(
            CONCAT(
                test_id,
                detalhe_exame
            )
        ) AS sample_id,
        test_id,
        date_testing,
        CASE
            WHEN age > 120 OR age < 0 THEN NULL
            ELSE age
        END AS age,
        CASE
            WHEN sex ILIKE 'F%' THEN 'F'
            WHEN sex ILIKE 'M%' THEN 'M'
            ELSE NULL
        END AS sex,
        
        detalhe_exame AS original_test_name,
        NULL AS original_test_detail_name,

        -- WIP
        'TESTE WIP KIT' AS test_kit,
        'TESTE WIP PAINEL 24 PATÓGENOS' AS test_name,
        'PCR' AS test_methodology,
        true AS is_a_multipathogen_test,

        location,
        state_code,

        {{ map_state_code_to_state_name('state_code', 'NULL') }} AS state,

        {% for column_name in result_column_names %}
            CASE 
                WHEN "{{ column_name }}" ILIKE 'NAO DETECTADO%' THEN 0
                WHEN "{{ column_name }}" ILIKE 'DETECTADO%'     THEN 1
                WHEN "{{ column_name }}" ILIKE 'INCONCLUSIVO'   THEN -1
                -- Em outra pipeline, o teste com 'INCONCLUSIVO' poderia simplesmente ser removido
                -- Nesta, ele precisa se mapeado para -1, já que a linha não pode ser removida

                WHEN "{{ column_name }}" IS NULL                THEN -1
                ELSE -2 --UNKNOWN
            END AS "{{ column_name }}",
        {% endfor %}
        file_name
    FROM source_data
    WHERE
        {% for column_name in result_column_names %}
            (
                "{{ column_name }}" NOT IN ('INCONCLUSIVO')
                AND "{{ column_name }}" IS NOT NULL
            )
            {% if not loop.last %}
            OR
            {% endif %}
        {% endfor %}
),
unpivot_pathogens AS (

    SELECT 
        sample_id, test_id, date_testing, age, sex, location, state_code, state,
        original_test_name, original_test_detail_name, is_a_multipathogen_test,
        test_kit, 
        
        'INFLUENZA_A'     AS pathogen_name,
        'H1N1'            AS pathogen_detail,
        result_virus_h1n1 AS pathogen_result
    FROM fix_values 
)
SELECT * FROM unpivot_pathogens