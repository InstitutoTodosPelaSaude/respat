{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM
    {{ ref("hlagyn_02_fix_values") }}
)
SELECT
    sample_id,
    test_id,
    date_testing,
    age,
    sex,
    detalhe_exame,
    location,
    state_code,
    state,
    test_kit,
    file_name,
    -- SC2 RESULTS
    greatest(
        result_covid,
        result_virus_sars_cov_2,
        result_virus_cov2,
        result_virus_sars
    ) AS "SC2_test_result",

    -- FLUA RESULTS
    greatest(
        result_virus_influenza_a,
        result_virus_h1n1,
        result_virus_ah3,
        result_virus_ia
    ) AS "FLUA_test_result",

    -- FLUB RESULTS
    greatest(
        result_virus_influenza_b,
        result_virus_b
    ) AS "FLUB_test_result",

    -- VSR RESULTS
    greatest(
        result_virus_sincicial_respiratorio,
        result_virus_sa,
        result_virus_sb
    ) AS "VSR_test_result",

    -- META RESULTS
    greatest(
        result_virus_mh
    ) AS "META_test_result",

    -- RINO RESULTS
    greatest(
        result_virus_rh
    ) AS "RINO_test_result",

    -- PARA RESULTS
    greatest(
        result_virus_ph,
        result_virus_ph2,
        result_virus_ph3,
        result_virus_ph4
    ) AS "PARA_test_result",

    -- ADENO RESULTS
    greatest(
        result_virus_ade
    ) AS "ADENO_test_result",

    -- BOCA RESULTS
    greatest(
        result_virus_boc
    ) AS "BOCA_test_result",

    -- COVS RESULTS
    greatest(
        result_virus_229e,
        result_virus_hku,
        result_virus_nl63,
        result_virus_oc43
    ) AS "COVS_test_result",

    -- ENTERO RESULTS
    greatest(
        result_virus_ev
    ) AS "ENTERO_test_result",

    -- BAC RESULTS
    greatest(
        result_bacte_bp,
        result_bacte_bpar,
        result_bacte_mp
    ) AS "BAC_test_result"
FROM source_data