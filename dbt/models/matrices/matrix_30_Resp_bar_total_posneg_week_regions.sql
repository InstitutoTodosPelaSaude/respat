{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-01' %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        region,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        epiweek_enddate >= '{{ epiweek_start }}' AND
        test_kit NOT IN (
            'adeno_iga', 
            'adeno_igg', 
            'adeno_igm',
            'bac_igg', 
            'bac_igm', 
            'bac_iga', 
            'bac_antibodies',
            'covid_antibodies', 
            'covid_iga',
            'flua_igg', 
            'flua_igm',
            'flub_igg', 
            'flub_igm',
            'sc2_igg',
            'vsr_igg'
        ) AND 
        region != 'NOT REPORTED'
    GROUP BY epiweek_enddate, region
    ORDER BY epiweek_enddate, region
)

SELECT
    epiweek_enddate as "Semana Epidemiológica",
    MAX(CASE WHEN region = 'Centro-Oeste' THEN "totaltests" ELSE 0 END) as "Centro-Oeste",
    MAX(CASE WHEN region = 'Nordeste' THEN "totaltests" ELSE 0 END) as "Nordeste",
    MAX(CASE WHEN region = 'Norte' THEN "totaltests" ELSE 0 END) as "Norte",
    MAX(CASE WHEN region = 'Sudeste' THEN "totaltests" ELSE 0 END) as "Sudeste",
    MAX(CASE WHEN region = 'Sul' THEN "totaltests" ELSE 0 END) as "Sul"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate