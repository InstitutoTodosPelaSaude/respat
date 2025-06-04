{{ config(materialized='table') }}

{% set month_start = '2022-01' %}

WITH source_data AS (
    SELECT
        "month",
        region,
        COUNT(DISTINCT sample_id) AS distinct_tests
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "month" >= '{{ month_start }}' AND
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
    GROUP BY "month", region
    ORDER BY "month", region
)

SELECT
    "month" as "Ano-Mês",
    SUM(CASE WHEN region = 'Centro-Oeste' THEN "distinct_tests" ELSE 0 END) as "Centro-Oeste",
    SUM(CASE WHEN region = 'Nordeste' THEN "distinct_tests" ELSE 0 END) as "Nordeste",
    SUM(CASE WHEN region = 'Norte' THEN "distinct_tests" ELSE 0 END) as "Norte",
    SUM(CASE WHEN region = 'Sudeste' THEN "distinct_tests" ELSE 0 END) as "Sudeste",
    SUM(CASE WHEN region = 'Sul' THEN "distinct_tests" ELSE 0 END) as "Sul"
FROM source_data
GROUP BY "month"
ORDER BY "month"