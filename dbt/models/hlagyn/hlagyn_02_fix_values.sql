

{{ config(materialized='table') }}
{%
    set state_code_to_state_name = {
        'AC': 'Acre',
        'AL': 'Alagoas',
        'AP': 'Amapá',
        'AM': 'Amazonas',
        'BA': 'Bahia',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'ES': 'Espírito Santo',
        'GO': 'Goiás',
        'MA': 'Maranhão',
        'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais',
        'PA': 'Pará',
        'PB': 'Paraíba',
        'PR': 'Paraná',
        'PE': 'Pernambuco',
        'PI': 'Piauí',
        'RJ': 'Rio de Janeiro',
        'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul',
        'RO': 'Rondônia',
        'RR': 'Roraima',
        'SC': 'Santa Catarina',
        'SP': 'São Paulo',
        'SE': 'Sergipe',
        'TO': 'Tocantins'
    }
%}

{% set result_column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('hlagyn_01_convert_types'), except=["test_id", "date_testing", "age", "sex", "detalhe_exame", "location", "state_code", "file_name"]) %}

WITH source_data AS (
    SELECT * 
    FROM {{ ref('hlagyn_01_convert_types') }}
)
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
    detalhe_exame,
    location,
    state_code,
    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,
    CASE 
        WHEN result_covid IS NOT NULL THEN 'covid_pcr'
        WHEN result_virus_influenza_a IS NOT NULL THEN 'test_4'
        WHEN result_virus_ia IS NOT NULL THEN 'test_24'
        ELSE 'UNKNOWN'
    END AS test_kit,
    {% for column_name in result_column_names %}
        CASE 
            WHEN "{{ column_name }}" ILIKE 'NAO DETECTADO%' THEN 0
            WHEN "{{ column_name }}" ILIKE 'DETECTADO%'     THEN 1
            WHEN "{{ column_name }}" IS NULL                THEN -1
            ELSE -2 --UNKNOWN
        END AS "{{ column_name }}",
    {% endfor %}
    file_name
FROM source_data
WHERE
    {% for column_name in result_column_names %}
        "{{ column_name }}" NOT IN 
        (
            'INCONCLUSIVO' 
        )
        {% if not loop.last %}
        AND
        {% endif %}
    {% endfor %}