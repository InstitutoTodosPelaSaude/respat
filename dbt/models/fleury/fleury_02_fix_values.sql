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
    SELECT * FROM 
    {{ ref('fleury_01_convert_types') }}
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
    patient_id,
    sex,
    CASE
        WHEN regexp_like(age, '^[0-9]*$') THEN CAST(age AS INT)                     -- Examples: '100', '95'
        WHEN regexp_like(age, '^[0-9]*A') THEN CAST(SPLIT_PART(age, 'A', 1) AS INT) -- Examples: '100A', '95A10M'
        WHEN regexp_like(age, '^[0-9]*M') THEN 0                                    -- Examples: '11M', '11A10D'
        WHEN regexp_like(age, '^[0-9]*D') THEN 0                                    -- Examples: '11D', '30D'
        WHEN age IS NULL THEN NULL                                                  -- Examples: NULL
        ELSE -1                                                                     -- Avoid missing new formats
    END AS age,
    location,
    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,
    exame,
    CASE exame
        WHEN 'FLUABRSVGX'       THEN 'test_3'
        WHEN 'COVIDFLURSVGX'    THEN 'test_4'
        WHEN 'VIRUSMOL'         THEN 'test_21'
        WHEN '2019NCOV'         THEN 'covid_pcr'
        WHEN 'COVID19POCT'      THEN 'covid_pcr'
        WHEN '2019NCOVTE'       THEN 'covid_pcr'
        WHEN 'AGCOVIDNS'        THEN 'covid_antigen'
        WHEN 'AGINFLU'          THEN 'flu_antigen'
        WHEN 'AGSINCURG'        THEN 'vsr_antigen'
        WHEN 'COVID19GX'        THEN 'covid_pcr'
        WHEN 'COVID19SALI'      THEN 'covid_pcr'
        WHEN 'INFLUENZAPCR'     THEN 'flu_pcr'
        WHEN 'VRSAG'            THEN 'vsr_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    pathogen,
    result,
    file_name
FROM source_table
WHERE 
    exame NOT IN (
        'ZIKAVIGM',
        'AAFAMARELA',
        'FAMARELAPCR',
        'ZIKAVPCR',
        'DENGUEMTE',
        'AACHIKV',
        'AGDENGUE',
        'CHIKVPCR',
        'DENGUEG',
        'DENGUEGTE',
        'DENGUEM',
        'DENGUENS1',
        'ZIKAVIGG',
        'ZIKAVPCR',
        'MAYAVPCR',
        'OROVPCR',
        'AAMAYAV',
        'AAOROV'
    ) 
    AND test_id not in ('7000800337_1800')
    AND result not in ('KIT SD BIOLINE INFLUENZA A+B')
    AND pathogen not in ('INFLUENZA A E B - TESTE RAPIDO - KIT')