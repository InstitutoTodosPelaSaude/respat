

{{ config(materialized='table') }}

{% set result_column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('hlagyn_01_convert_types'), except=["test_id", "date_testing", "age", "sex", "detalhe_exame", "location", "state_code", "file_name"]) %}

{%
    set pathogen_names_and_result_columns = [

        ('SARS_COV_2' ,      'VIRUS',    'result_covid',          NULL),


        ('INFLUENZA_A',      'VIRUS',    'result_virus_influenza_a',         NULL),
        ('INFLUENZA_B',      'VIRUS',    'result_virus_influenza_b',         NULL),
        ('VSR',              'VIRUS',    'result_virus_sincicial_respiratorio',NULL),
        ('SARS_COV_2' ,      'VIRUS',    'result_virus_sars_cov_2',          NULL),


        ('INFLUENZA_A',      'VIRUS',    'result_virus_ia',      NULL),
        ('INFLUENZA_A',      'VIRUS',    'result_virus_h1n1',    'H1N1'),
        ('INFLUENZA_A',      'VIRUS',    'result_virus_ah3',     'AH3'),

        ('INFLUENZA_B',      'VIRUS',    'result_virus_b',       NULL),

        ('METAPNEUMOVIRUS',  'VIRUS',    'result_virus_mh',      NULL),

        ('VSR',              'VIRUS',    'result_virus_sb',      'VSRB'),
        ('VSR',              'VIRUS',    'result_virus_sa',      'VSRA'),

        ('RINOVIRUS',        'VIRUS',    'result_virus_rh',      NULL),

        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph',     'PH1'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph2',    'PH2'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph3',    'PH3'),
        ('PARAINFLUENZA',    'VIRUS',    'result_virus_ph4',    'PH4'),

        ('CORONAVIRUS_229E', 'VIRUS',    'result_virus_229e',   NULL),
        ('CORONAVIRUS_HKU',  'VIRUS',    'result_virus_hku',    NULL),
        ('CORONAVIRUS_NL63', 'VIRUS',    'result_virus_nl63',   NULL),
        ('CORONAVIRUS_OC43', 'VIRUS',    'result_virus_oc43',   NULL),

        ('SARS_COV_2' ,      'VIRUS',    'result_virus_sars',   NULL),
        ('SARS_COV_2' ,      'VIRUS',    'result_virus_cov2',   NULL),

        ('ADENOVIRUS',       'VIRUS',    'result_virus_ade',    NULL),
        ('BOCAVIRUS',        'VIRUS',    'result_virus_boc',    NULL),
        ('ENTEROVIRUS',      'VIRUS',    'result_virus_ev',     NULL),

        ('B_PERTUSSIS',      'BACTERIA', 'result_bacte_bp',     NULL),
        ('B_PARAPERTUSIS',   'BACTERIA', 'result_bacte_bpar',   NULL),
        ('M_PNEUMONIAE',     'BACTERIA', 'result_bacte_mp',     NULL),
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