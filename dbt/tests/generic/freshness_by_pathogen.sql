{% test freshness_by_pathogen(model) %}

    {%set test_result_columns = [
        '"SC2_test_result"',
        '"FLUA_test_result"',
        '"FLUB_test_result"',
        '"VSR_test_result"',
        '"META_test_result"',
        '"RINO_test_result"',
        '"PARA_test_result"',
        '"ADENO_test_result"',
        '"BOCA_test_result"',
        '"COVS_test_result"',
        '"ENTERO_test_result"',
        '"BAC_test_result"'
        ]
    %}

    SELECT
    *
    FROM
    (
        {% for pathogen in test_result_columns %}
            SELECT
                MAX(date_testing) AS max_date_testing
            FROM {{ model }}
            WHERE {{ pathogen }} != 'NT'
            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
    ) AS max_dates
    WHERE max_date_testing < CURRENT_DATE - INTERVAL '5 days'

{% endtest %}