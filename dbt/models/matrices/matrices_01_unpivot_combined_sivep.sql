{{
    config(
        materialized='view'
    )
}}

WITH 
age_groups AS (
    SELECT * 
    FROM {{ ref("age_groups") }}
),

epiweeks AS (
    SELECT *
    FROM {{ ref("epiweeks") }}
),

macroregions AS (
    SELECT DISTINCT {{ normalize_text("state_name") }} AS state_name, region
    FROM {{ ref("macroregions") }}
),

source_data_raw AS(
    SELECT
    *
    FROM {{ ref("sivep_final") }}
),

source_data_epiweeks AS(
    SELECT 
        source_data_raw.*,
        ew.end_date as epiweek_enddate,
        ew.week_num as epiweek_number,

        TO_CHAR(source_data_raw.date_pri_sin, 'YYYY-MM') as month
        
    FROM source_data_raw
    LEFT JOIN epiweeks AS ew ON source_data_raw.date_pri_sin >= ew.start_date AND source_data_raw.date_pri_sin <= ew.end_date
),

source_data_regions AS (
    SELECT 
        source_data_epiweeks.*,
        r.region AS region
    FROM source_data_epiweeks
    LEFT JOIN macroregions AS r ON source_data_epiweeks.state = r.state_name
),

source_data_full AS(
    SELECT 
        source_data_regions.*,
        ag.age_group
    FROM source_data_regions
    LEFT JOIN age_groups AS ag ON source_data_regions.age >= ag." min_age" AND source_data_regions.age <=  ag." max_age"
),
 
source_data AS (
    SELECT
        sample_id,
        test_kit,
        epiweek_enddate,
        epiweek_number,
        region,
        CASE WHEN age_group IS NULL THEN 'NOT REPORTED' ELSE age_group END AS age_group,
        "SC2_test_result",
        "FLUA_test_result",
        "FLUB_test_result",
        "VSR_test_result",
        "COVS_test_result",
        "ADENO_test_result",
        "BOCA_test_result",
        "RINO_test_result",
        "PARA_test_result",
        "ENTERO_test_result",
        "META_test_result",
        "BAC_test_result"
    FROM source_data_full
    WHERE 
        epiweek_enddate < CURRENT_DATE AND
        epiweek_enddate >= '2022-01-01'
)

SELECT
    combined.*,
    combined_pivoted.*
FROM
    source_data combined
CROSS JOIN LATERAL (
    VALUES
        (combined."SC2_test_result",    'SC2'),
        (combined."FLUA_test_result",   'FLUA'),
        (combined."FLUB_test_result",   'FLUB'),
        (combined."VSR_test_result",    'VSR'),
        (combined."COVS_test_result",   'COVS'),
        (combined."ADENO_test_result",  'ADENO'),
        (combined."BOCA_test_result",   'BOCA'),
        (combined."RINO_test_result",   'RINO'),
        (combined."PARA_test_result",   'PARA'),
        (combined."ENTERO_test_result", 'ENTERO'),
        (combined."META_test_result",   'META'),
        (combined."BAC_test_result",    'BAC')
) AS combined_pivoted(result, pathogen)
