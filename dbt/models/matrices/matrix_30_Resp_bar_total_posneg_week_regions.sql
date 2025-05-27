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
        test_kit IN (
            'adeno_pcr',
            'bac_antigen',
            'bac_pcr',
            'covid_antigen',
            'covid_pcr',
            'flu_antigen',
            'flu_pcr',
            'test_14',
            'test_21',
            'test_23',
            'test_24',
            'test_3',
            'test_4',
            'thermo',
            'vsr_antigen'
        ) AND 
        region != 'NOT REPORTED'
    GROUP BY epiweek_enddate, region
    ORDER BY epiweek_enddate, region
)

SELECT
    epiweek_enddate as "Semana Epidemiológica",
    SUM(CASE WHEN region = 'Centro-Oeste' THEN "totaltests" ELSE 0 END) as "Centro-Oeste",
    SUM(CASE WHEN region = 'Nordeste' THEN "totaltests" ELSE 0 END) as "Nordeste",
    SUM(CASE WHEN region = 'Norte' THEN "totaltests" ELSE 0 END) as "Norte",
    SUM(CASE WHEN region = 'Sudeste' THEN "totaltests" ELSE 0 END) as "Sudeste",
    SUM(CASE WHEN region = 'Sul' THEN "totaltests" ELSE 0 END) as "Sul"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate