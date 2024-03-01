{% test only_one_test_result_is_present(model) %}

    SELECT
        *
    FROM {{ model }}
    WHERE
        NOT (
            "SC2_test_result" +
            "FLUA_test_result" +
            "FLUB_test_result" +
            "VSR_test_result" +
            "META_test_result" +
            "RINO_test_result" +
            "PARA_test_result" +
            "ADENO_test_result" +
            "BOCA_test_result" +
            "COVS_test_result" +
            "ENTERO_test_result" +
            "BAC_test_result"
        ) IN (-11, -10)
        -- Because the test results are -1 for not tested, 0 for negative and 1 for positive
        -- the only way to have a sum of -11 or -10 is if there is only one test result present (0 or 1)

{% endtest %}