{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "sivep_raw") }}
)
SELECT 
    nm_un_inte as id_unidade,

    -- PACIENT
    cs_sexo AS sex,
    nu_idade_n::NUMERIC::INTEGER AS nu_idade_n, 
    tp_idade::NUMERIC::INTEGER   AS tp_idade,
    
    -- LOCATION
    co_mun_res::NUMERIC::INTEGER        AS co_mun_res,
    {{ normalize_text("id_mn_resi") }}  AS location,
    {{ normalize_text("sg_uf") }}       AS sg_uf,
    id_pais,
    id_rg_resi,

    TO_DATE(dt_coleta, 'yyyy-mm-dd') AS date_testing,

    sem_pri::NUMERIC::INTEGER AS sem_pri,
    amostra::NUMERIC::INTEGER AS amostra,
    
    TO_DATE(dt_notific, 'yyyy-mm-dd') AS dt_notific ,
    TO_DATE(dt_sin_pri, 'yyyy-mm-dd') AS dt_sin_pri , 
    TO_DATE(dt_res_an, 'yyyy-mm-dd') AS dt_res_an ,
    res_an::NUMERIC::INTEGER AS res_an,
    pos_an_flu::NUMERIC::INTEGER AS pos_an_flu,
    tp_flu_an::NUMERIC::INTEGER AS tp_flu_an,
    pos_an_out::NUMERIC::INTEGER AS pos_an_out ,
    an_sars2::NUMERIC::INTEGER AS an_sars2 ,
    an_vsr::NUMERIC::INTEGER AS an_vsr ,
    an_para1::NUMERIC::INTEGER AS an_para1 ,
    an_para2::NUMERIC::INTEGER AS an_para2 ,
    an_para3::NUMERIC::INTEGER AS an_para3 ,
    an_adeno::NUMERIC::INTEGER AS an_adeno ,
    an_outro::NUMERIC::INTEGER AS an_outro ,
    ds_an_out,
    TO_DATE(dt_pcr, 'yyyy-mm-dd') AS dt_pcr,
    pcr_resul::NUMERIC::INTEGER AS pcr_resul,
    pos_pcrflu::NUMERIC::INTEGER AS pos_pcrflu,
    tp_flu_pcr::NUMERIC::INTEGER AS tp_flu_pcr,
    pos_pcrout::NUMERIC::INTEGER AS pos_pcrout,
    pcr_sars2::NUMERIC::INTEGER AS pcr_sars2,
    pcr_vsr::NUMERIC::INTEGER AS pcr_vsr,
    pcr_para1::NUMERIC::INTEGER AS pcr_para1,
    pcr_para2::NUMERIC::INTEGER AS pcr_para2,
    pcr_para3::NUMERIC::INTEGER AS pcr_para3,
    pcr_para4::NUMERIC::INTEGER AS pcr_para4,
    pcr_adeno::NUMERIC::INTEGER AS pcr_adeno,
    pcr_metap::NUMERIC::INTEGER AS pcr_metap,
    pcr_boca::NUMERIC::INTEGER AS pcr_boca,
    pcr_rino::NUMERIC::INTEGER AS pcr_rino,
    pcr_outro::NUMERIC::INTEGER AS pcr_outro,
    ds_pcr_out,
    classi_fin::NUMERIC::INTEGER AS classi_fin,
    classi_out,
    REPLACE(criterio, ';', '')::NUMERIC::INTEGER AS criterio ,
    
    file_name
FROM source_data