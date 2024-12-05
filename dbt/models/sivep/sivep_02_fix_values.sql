{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("sivep_01_convert_types") }}
),
results_normalized AS (
    SELECT
        id_unidade,

        -- PACIENT INFO
        CASE 
            WHEN sex NOT IN ('F', 'M') THEN NULL
            ELSE sex
        END AS sex,
        
        CASE
            -- tp_idade = 1 (age 0-30 days), tp_idade = 2 (age 1-12 months), tp_idade=3 (age in years)
            WHEN tp_idade IN (1, 2) THEN 0
            WHEN tp_idade = 3 THEN nu_idade_n
            ELSE NULL
        END AS age,
        
        -- LOCATION
        co_mun_res,
        location,
        {{ map_state_code_to_state_name('sg_uf', 'NULL') }} AS state,
        id_pais,
        id_rg_resi,

        date_testing,

        -- Itens 69 - Dicionário de dados SRAG
        CASE
            WHEN TP_FLU_AN=1 AND POS_AN_FLU=1 THEN 1
            WHEN TP_FLU_AN=1 AND POS_AN_FLU=2 THEN 0
            ELSE NULL
        END AS "FLUA_antigen_result",
        CASE
            WHEN TP_FLU_AN=2 AND POS_AN_FLU=1 THEN 1
            WHEN TP_FLU_AN=2 AND POS_AN_FLU=2 THEN 0
            ELSE NULL
        END AS "FLUB_antigen_result",
        AN_SARS2 AS "SC2_antigen_result",
        AN_VSR   AS "VSR_antigen_result",
        CASE
            WHEN AN_PARA1=1 OR AN_PARA2=1 OR AN_PARA3=1 THEN 1
            ELSE NULL
        END AS "PARA_antigen_result",
        AN_ADENO AS "ADENO_antigen_result", 

        -- Itens 72 - Dicionário de dados SRAG
        CASE
            WHEN TP_FLU_PCR=1 AND POS_PCRFLU=1 THEN 1
            WHEN TP_FLU_PCR=1 AND POS_PCRFLU=2 THEN 0
            ELSE NULL
        END AS "FLUA_pcr_result",
        CASE
            WHEN TP_FLU_PCR=2 AND POS_PCRFLU=1 THEN 1
            WHEN TP_FLU_PCR=2 AND POS_PCRFLU=2 THEN 0
            ELSE NULL
        END AS "FLUB_pcr_result",

        pcr_sars2 AS "SC2_pcr_result",
        pcr_vsr AS "VSR_pcr_result",
        pcr_adeno AS "ADENO_pcr_result",
        pcr_metap AS "META_pcr_result",
        pcr_boca AS "BOCA_pcr_result",
        pcr_rino AS "RINO_pcr_result",

        CASE
            WHEN PCR_PARA1=1 OR PCR_PARA2=1 OR PCR_PARA3=1 OR PCR_PARA4=1 THEN 1
            ELSE NULL
        END AS "PARA_pcr_result",
        
        file_name
        
    FROM source_data
    WHERE 1=1
        AND amostra = 1 -- Only tests with sample collected
        AND classi_fin != 4 -- Remove Non-specified SRAG
        AND location IS NOT NULL
        AND date_testing <= CURRENT_DATE
        AND date_testing IS NOT NULL
)


