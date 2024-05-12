{% test test_kit_contains_the_correct_pathogen_results(model) %}

    {%
        set test_kit_pathogens = [
            ['adeno_antigen', [['ADENO'], ['ADENO']] ],
            ['adeno_igg',     [['ADENO']] ],
            ['adeno_igm',     [['ADENO']] ],
            ['adeno_test',    [['ADENO']] ],
            ['bac_antigen',   [['BAC']] ],
            ['bac_igg',       [['BAC']] ],
            ['bac_igm',       [['BAC']] ],
            ['bac_test',      [['BAC']] ],
            ['flua_igg',      [['FLUA']] ],
            ['flua_igm',      [['FLUA']] ],
            ['flub_igg',      [['FLUB']] ],
            ['flub_igm',      [['FLUB']] ],
            ['flu_pcr',       [['FLUA', 'FLUB']] ],
            ['covid_antigen', [['SC2']] ],
            ['sc2_antigen',   [['SC2']] ],
            ['sc2_igg',       [['SC2']] ],
            ['test_24',       [['SC2','FLUA','FLUB','VSR','META','RINO','PARA','ADENO','BOCA','COVS','ENTERO','BAC']] ],
            ['test_4',        [['SC2','FLUA','FLUB','VSR']] ],
            ['vsr_igg',       [['VSR']] ],
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