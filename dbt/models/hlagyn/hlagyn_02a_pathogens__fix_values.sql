

{{ config(materialized='table') }}

{% set result_column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('hlagyn_01_convert_types'), except=["test_id", "date_testing", "age", "sex", "detalhe_exame", "location", "state_code", "file_name"]) %}

{%
    set pathogen_names_and_result_columns = [

        ('SARS_COV_2' ,      'VIRUS',    'result_covid',          'NO_DETAIL'),


        ('INFLUENZA_A',      'VIRUS',    'result_virus_influenza_a',         'NO_DETAIL'),
        ('INFLUENZA_B',      'VIRUS',    'result_virus_influenza_b',         'NO_DETAIL'),
        ('VSR',              'VIRUS',    'result_virus_sincicial_respiratorio','NO_DETAIL'),
        ('SARS_COV_2' ,      'VIRUS',    'result_virus_sars_cov_2',          'NO_DETAIL'),


        ('INFLUENZA_A',      'VIRUS',    'result_virus_ia',      'NO_DETAIL'),
        ('INFLUENZA_A',      'VIRUS',    'result_virus_h1n1',    'H1N1'),
        ('INFLUENZA_A',      'VIRUS',    'result_virus_ah3',     'AH3'),

        ('INFLUENZA_B',      'VIRUS',    'result_virus_b',       'NO_DETAIL'),

        ('METAPNEUMOVIRUS',  'VIRUS',    'result_virus_mh',      'NO_DETAIL'),

        ('VSR',              'VIRUS',    'result_virus_sb',      'VSRB'),
        ('VSR',              'VIRUS',    'result_virus_sa',      'VSRA'),

        ('RINOVIRUS',        'VIRUS',    'result_virus_rh',      'NO_DETAIL'),

        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph',     'PH1'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph2',    'PH2'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph3',    'PH3'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph4',    'PH4'),

        ('CORONAVIRUS_229E', 'VIRUS',    'result_virus_229e',   'NO_DETAIL'),
        ('CORONAVIRUS_HKU',  'VIRUS',    'result_virus_hku',    'NO_DETAIL'),
        ('CORONAVIRUS_NL63', 'VIRUS',    'result_virus_nl63',   'NO_DETAIL'),
        ('CORONAVIRUS_OC43', 'VIRUS',    'result_virus_oc43',   'NO_DETAIL'),

        ('SARS_COV_2' ,      'VIRUS',    'result_virus_sars',   'NO_DETAIL'),
        ('SARS_COV_2' ,      'VIRUS',    'result_virus_cov2',   'NO_DETAIL'),

        ('ADENOVIRUS',       'VIRUS',    'result_virus_ade',    'NO_DETAIL'),
        ('BOCAVIRUS',        'VIRUS',    'result_virus_boc',    'NO_DETAIL'),
        ('ENTEROVIRUS',      'VIRUS',    'result_virus_ev',     'NO_DETAIL'),

        ('B_PERTUSSIS',      'BACTERIA', 'result_bacte_bp',     'NO_DETAIL'),
        ('B_PARAPERTUSIS',   'BACTERIA', 'result_bacte_bpar',   'NO_DETAIL'),
        ('M_PNEUMONIAE',     'BACTERIA', 'result_bacte_mp',     'NO_DETAIL'),
    ]
%}

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
        CASE
            WHEN "file_name" ILIKE '%covid%' THEN 'WIP_kit_covid'
            WHEN "file_name" ILIKE '%pr3%'   THEN 'WIP_kit_pr3'
            WHEN "file_name" ILIKE '%pr4%'   THEN 'WIP_kit_pr4'
            WHEN "file_name" ILIKE '%pr24%'  THEN 'WIP_kit_pr24'
            ELSE NULL
        END AS test_kit,

        CASE
            WHEN "file_name" ILIKE '%covid%' THEN 'WIP TESTE COVID'
            WHEN "file_name" ILIKE '%pr3%'   THEN 'WIP TESTE PAINEL 3 PATÓGENOS'
            WHEN "file_name" ILIKE '%pr4%'   THEN 'WIP TESTE PAINEL 4 PATÓGENOS'
            WHEN "file_name" ILIKE '%pr24%'  THEN 'WIP TESTE PAINEL 24 PATÓGENOS'
            ELSE NULL
        END AS test_name,

        CASE
            WHEN "file_name" ILIKE '%covid%' THEN false
            WHEN "file_name" ILIKE '%pr3%'   THEN true
            WHEN "file_name" ILIKE '%pr4%'   THEN true
            WHEN "file_name" ILIKE '%pr24%'  THEN true
            ELSE NULL
        END AS is_a_multipathogen_test,
        'PCR' AS test_methodology,

        location,
        state_code,

        {{ map_state_code_to_state_name('state_code', 'NULL') }} AS state,

        {% for column_name in result_column_names %}
            CASE 
                
                WHEN "{{ column_name }}" ILIKE 'NAO DETECTADO%' THEN 0
                WHEN "{{ column_name }}" ILIKE 'DETECTADO%'     THEN 1
                WHEN "{{ column_name }}" ILIKE 'INCONCLUSIVO'   THEN -1

                -- In some cases, the result is writter like VIRUS_IA: Não detectado
                WHEN "{{ column_name }}" ILIKE '%: NAO DETECTADO%' THEN 0
                WHEN "{{ column_name }}" ILIKE '%: DETECTADO%'     THEN 1
                WHEN "{{ column_name }}" ILIKE '%: INCONCLUSIVO'   THEN -1

                WHEN "{{ column_name }}" IS NULL  THEN -1
                WHEN "{{ column_name }}" = ''     THEN -1

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

    {% for pathogen_name, pathogen_type, result_column, pathogen_detail in pathogen_names_and_result_columns %}
    SELECT
        sample_id,
        test_id,
        date_testing,
        age,
        sex,
        location,
        state_code,
        state,

        original_test_name,
        original_test_detail_name,
        
        test_kit,
        test_name,
        is_a_multipathogen_test,
        test_methodology,

        '{{ pathogen_name }}'  AS pathogen_name,
        
        {% if pathogen_detail %} 
            '{{ pathogen_detail }}' 
        {% else %}
            NULL
        {% endif %} AS pathogen_detail,

        '{{ pathogen_type }}'  AS pathogen_type,
        {{ result_column }}    AS pathogen_result

    FROM fix_values
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)
SELECT * FROM unpivot_pathogens