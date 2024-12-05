{% test test_kit_contains_the_correct_pathogen_results(model) %}

    {%
        set test_kit_pathogens = [
            ['adeno_iga',           [['ADENO']] ],
            ['adeno_igg',           [['ADENO']] ],
            ['adeno_igm',           [['ADENO']] ],
            ['adeno_pcr',           [['ADENO']] ],
            ['adeno_antigen',       [['ADENO']] ],
            ['bac_antigen',         [['BAC']] ],
            ['bac_igg',             [['BAC']] ],
            ['bac_igm',             [['BAC']] ],
            ['bac_pcr',             [['BAC']] ],
            ['covid_antibodies',    [['SC2']] ],
            ['covid_antigen',       [['SC2']] ],
            ['covid_iga',           [['SC2']] ],
            ['covid_pcr',           [['SC2']] ],
            ['flu_antigen',         [['FLUA', 'FLUB']]],
            ['flua_antigen',        [['FLUA']]],
            ['flub_antigen',        [['FLUB']]],
            ['flu_pcr',             [['FLUA', 'FLUB']] ],
            ['flua_pcr',            [['FLUA']] ],
            ['flub_pcr',            [['FLUB']] ],
            ['flua_igg',            [['FLUA']] ],
            ['flua_igm',            [['FLUA']] ],
            ['flub_igg',            [['FLUB']] ],
            ['flub_igm',            [['FLUB']] ],
            ['sc2_igg',             [['SC2']] ],
            ['vsr_antigen',         [['VSR']] ],
            ['vsr_igg',             [['VSR']] ],
            ['vsr_pcr',             [['VSR']] ],
            ['para_antigen',        [['PARA']] ],
            ['para_pcr',            [['PARA']] ],
            ['meta_pcr',            [['META']] ],
            ['boca_pcr',            [['BOCA']] ],
            ['rino_pcr',            [['RINO']] ],
            ['test_2',              [['FLUA','FLUB']] ],
            ['test_3',              [['FLUA','FLUB','VSR']] ],
            ['test_4',              [['SC2','FLUA','FLUB','VSR']] ],
            ['test_14',             [['SC2','FLUA','FLUB','VSR','META','RINO','PARA','ADENO','BOCA','COVS','ENTERO']] ],
            ['test_21',             [['SC2','FLUA','FLUB','VSR','META','RINO','PARA','ADENO','COVS','BAC']] ],
            ['test_24',             [['SC2','FLUA','FLUB','VSR','META','RINO','PARA','ADENO','BOCA','COVS','ENTERO','BAC']] ],
        ]
    %}

    {%
        set all_pathogens = [
            'SC2',
            'FLUA',
            'FLUB',
            'VSR',
            'META',
            'RINO',
            'PARA',
            'ADENO',
            'BOCA',
            'COVS',
            'ENTERO',
            'BAC'
        ]
    %}

    WITH test_results AS (
        SELECT
            CASE "SC2_test_result"    WHEN 'NT' THEN 0 ELSE 1 END AS SC2,
            CASE "FLUA_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS FLUA,
            CASE "FLUB_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS FLUB,
            CASE "VSR_test_result"    WHEN 'NT' THEN 0 ELSE 1 END AS VSR,
            CASE "META_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS META,
            CASE "RINO_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS RINO,
            CASE "PARA_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS PARA,
            CASE "ADENO_test_result"  WHEN 'NT' THEN 0 ELSE 1 END AS ADENO,
            CASE "BOCA_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS BOCA,
            CASE "COVS_test_result"   WHEN 'NT' THEN 0 ELSE 1 END AS COVS,
            CASE "ENTERO_test_result" WHEN 'NT' THEN 0 ELSE 1 END AS ENTERO,
            CASE "BAC_test_result"    WHEN 'NT' THEN 0 ELSE 1 END AS BAC,
            test_kit
        FROM {{model}}
    ),
    test_results_with_validation AS (
        
        SELECT
            *,
            CASE 
            {% for test_kit, accepted_pathogen_list in test_kit_pathogens %}
                WHEN test_kit = '{{test_kit}}' 
                THEN

                CASE 
                    {% for pathogen_list in accepted_pathogen_list %}
                        WHEN
                            {% for pathogen in all_pathogens %}
                                {% if pathogen in pathogen_list %} 
                                    {{pathogen}}=1 
                                {% else %}
                                    {{pathogen}}=0
                                {% endif %}
                                {% if not loop.last %}
                                    AND
                                {% endif %}
                            {% endfor %} 
                        THEN 'VALID'
                    {%endfor%}
                        ELSE 'INVALIDO'
                END

            {% endfor %}
                ELSE 'INVALIDO'
            END AS test_kit_validation
            FROM test_results

    )
    SELECT
        *
    FROM test_results_with_validation
    WHERE test_kit_validation = 'INVALIDO'

{% endtest %}