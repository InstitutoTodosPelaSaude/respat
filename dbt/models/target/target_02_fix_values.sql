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

WITH source_table AS (
    SELECT * 
    FROM {{ ref("target_01_convert_types") }}
)

SELECT
    md5(
        CONCAT(
            test_id,
            exame
        )
    ) AS sample_id,
    test_id,
    date_testing,
    CASE sex
        WHEN 'F' THEN 'F'
        WHEN 'M' THEN 'M'
        ELSE NULL
    END AS sex,
    CASE
        WHEN regexp_like(age, '^[0-9]*$') THEN CAST(age AS INT)                     -- Examples: '100', '95'
        WHEN regexp_like(age, '^[0-9]*A') THEN CAST(SPLIT_PART(age, 'A', 1) AS INT) -- Examples: '100A', '95A10M'
        WHEN regexp_like(age, '^[0-9]*M') THEN 0                                    -- Examples: '11M', '11M10D'
        WHEN regexp_like(age, '^[0-9]*D') THEN 0                                    -- Examples: '11D', '30D'
        WHEN age IS NULL THEN NULL                                                  -- Examples: NULL
    END AS age,
    location,
    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,
    exame,
    detalhe_exame,
    pathogen,
    CASE
        -- PAINEL RESPIRATORIO VIRAL + MYCOPLASMA E BORDETELLA
        WHEN exame IN ('RESCAP02') THEN 'test_23'
        -- RT-PCR CORONAVÍRUS SARS-COV ...
        WHEN exame IN ('COV19SWI','COVID19','COVID19U') THEN 'covid_pcr'
        -- PAINEL TRIVIRAL PARA COVID, FLU E RSV
        WHEN exame IN ('RESCAP04') THEN 'test_4'
    END AS test_kit,
    CASE 
        WHEN result = 'NAO DETECTADO' THEN 0
        WHEN result = 'DETECTADO'     THEN 1
        WHEN result ILIKE 'DETECTADO (PRESENCA DO RNA DE CORONAVIRUS SARS-COV-2%' THEN 1
    END AS result,
    file_name
FROM source_table
WHERE exame NOT IN ('DENVQL', 'CHIKVQL', 'ZIKVQL') -- REMOVE ARBO-RELATED TESTS