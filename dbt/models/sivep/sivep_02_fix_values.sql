{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ ref("sivep_01_convert_types") }}
)
SELECT
    id_unidade,

    -- PACIENT
    CASE 
        WHEN sex NOT IN ('F', 'M') THEN NULL
        ELSE sex
    END AS sex,
    
    CASE
        -- tp_idade = 1 (days, 0-30), tp_idade = 2 (months, 1-12), tp_idade=3 (years)
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

    sem_pri,
    amostra,
    
    dt_notific ,
    dt_sin_pri ,
    dt_res_an ,
    res_an,
    pos_an_flu,
    pos_an_out ,
    an_sars2 ,
    an_vsr ,
    an_para1 ,
    an_para2 ,
    an_para3 ,
    an_adeno ,
    an_outro ,
    ds_an_out,
    dt_pcr,
    pcr_resul,
    pos_pcrflu,
    pos_pcrout,
    pcr_sars2,
    pcr_vsr,
    pcr_para1,
    pcr_para2,
    pcr_para3,
    pcr_para4,
    pcr_adeno,
    pcr_metap,
    pcr_boca,
    pcr_rino,
    pcr_outro,
    ds_pcr_out ,
    classi_fin,
    classi_out,
    criterio ,
    
    file_name
FROM source_data
WHERE 1=1
AND amostra = 1 -- Only tests with sample collected
AND location IS NOT NULL
AND date_testing <= CURRENT_DATE
AND date_testing IS NOT NULL