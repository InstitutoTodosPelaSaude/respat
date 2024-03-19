

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

WITH source_data AS (

    SELECT
    *,
    ROW_NUMBER() OVER(
        PARTITION BY md5(
                        CONCAT(
                            test_id,
                            exame,
                            patient_id
                        )
                    )
        ORDER BY md5(
                    CONCAT(
                        test_id,
                        exame,
                        patient_id
                    )
                )
    ) AS row_number
    FROM {{ ref('hilab_01_convert_types') }}

)
SELECT
    md5(
        CONCAT(
            test_id,
            exame,
            patient_id
        )
    ) AS sample_id,
    test_id,
    state_code,
    location,
    date_testing,
    exame,
    CASE 
        WHEN exame = 'COVID-19 ANTIGENO' THEN 'covid_antigen'
        WHEN exame = 'INFLUENZA A' THEN 'flu_antigen'
        WHEN exame = 'INFLUENZA B' THEN 'flu_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    CASE
        WHEN result = 'NAO REAGENTE' THEN 0
        WHEN result = 'REAGENTE' THEN 1
        ELSE -2
    END AS result,

    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,

    CASE
        WHEN age > 120 OR age < 0 THEN NULL
        ELSE age
    END AS age,
    patient_id,
    CASE
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE NULL
    END AS sex,
    file_name
FROM source_data
-- This column is used to filter out duplicate rows
WHERE row_number = 1