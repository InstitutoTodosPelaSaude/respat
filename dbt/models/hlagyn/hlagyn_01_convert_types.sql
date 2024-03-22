{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "hlagyn_raw") }}
)
SELECT
    "Pedido"                                    AS test_id,
    TO_DATE("Data Coleta", 'YYYY-MM-DD')        AS date_testing,
    "Idade"::INT                                AS age,
    {{ normalize_text("Sexo") }}                AS sex,
    {{ normalize_text("Métodologia") }}         AS detalhe_exame,
    {{ normalize_text("Cidade") }}              AS location,
    {{ normalize_text("UF") }}                  AS state_code,

    {{ normalize_text("Resultado") }}           AS result_covid,

    {{ normalize_text("Vírus Influenza A") }}   AS result_virus_influenza_a,
    {{ normalize_text("Vírus Influenza B") }}   AS result_virus_influenza_b,
    {{ normalize_text("Vírus Sincicial Respiratório A/B") }} AS result_virus_sincicial_respiratorio,
    {{ normalize_text("Coronavírus SARS-CoV-2") }} AS result_virus_sars_cov_2,

    {{ normalize_text("VIRUS_IA") }}            AS result_virus_ia,
    {{ normalize_text("VIRUS_H1N1") }}          AS result_virus_h1n1,
    {{ normalize_text("VIRUS_AH3") }}           AS result_virus_ah3,
    {{ normalize_text("VIRUS_B") }}             AS result_virus_b,
    {{ normalize_text("VIRUS_MH") }}            AS result_virus_mh,
    {{ normalize_text("VIRUS_SA") }}            AS result_virus_sa,
    {{ normalize_text("VIRUS_SB") }}            AS result_virus_sb,
    {{ normalize_text("VIRUS_RH") }}            AS result_virus_rh,
    {{ normalize_text("VIRUS_PH") }}            AS result_virus_ph,
    {{ normalize_text("VIRUS_PH2") }}           AS result_virus_ph2,
    {{ normalize_text("VIRUS_PH3") }}           AS result_virus_ph3,
    {{ normalize_text("VIRUS_PH4") }}           AS result_virus_ph4,
    {{ normalize_text("VIRUS_ADE") }}           AS result_virus_ade,
    {{ normalize_text("VIRUS_BOC") }}           AS result_virus_boc,
    {{ normalize_text("VIRUS_229E") }}          AS result_virus_229e,
    {{ normalize_text("VIRUS_HKU") }}           AS result_virus_hku,
    {{ normalize_text("VIRUS_NL63") }}          AS result_virus_nl63,
    {{ normalize_text("VIRUS_OC43") }}          AS result_virus_oc43,
    {{ normalize_text("VIRUS_SARS") }}          AS result_virus_sars,
    {{ normalize_text("VIRUS_COV2") }}          AS result_virus_cov2,
    {{ normalize_text("VIRUS_EV") }}            AS result_virus_ev,
    {{ normalize_text("BACTE_BP") }}            AS result_bacte_bp,
    {{ normalize_text("BACTE_BPAR") }}          AS result_bacte_bpar,
    {{ normalize_text("BACTE_MP") }}            AS result_bacte_mp,

    file_name
FROM source_data