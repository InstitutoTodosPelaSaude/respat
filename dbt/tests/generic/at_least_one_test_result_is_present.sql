{% test at_least_one_test_result_is_present(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        "SC2_test_result" = -1 AND
        "FLUA_test_result" = -1 AND
        "FLUB_test_result" = -1 AND
        "VSR_test_result" = -1 AND
        "RINO_test_result" = -1 AND
        "PARA_test_result" = -1 AND
        "ADENO_test_result" = -1 AND
        "BOCA_test_result" = -1 AND
        "COVS_test_result" = -1 AND
        "ENTERO_test_result" = -1 AND
        "BAC_test_result" = -1

{% endtest %}