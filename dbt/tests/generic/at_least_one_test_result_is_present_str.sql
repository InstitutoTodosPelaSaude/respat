{% test at_least_one_test_result_is_present_str(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        "SC2_test_result" = 'NT' AND
        "FLUA_test_result" = 'NT' AND
        "FLUB_test_result" = 'NT' AND
        "VSR_test_result" = 'NT' AND
        "RINO_test_result" = 'NT' AND
        "META_test_result" = 'NT' AND
        "PARA_test_result" = 'NT' AND
        "ADENO_test_result" = 'NT' AND
        "BOCA_test_result" = 'NT' AND
        "COVS_test_result" = 'NT' AND
        "ENTERO_test_result" = 'NT' AND
        "BAC_test_result" = 'NT'

{% endtest %}