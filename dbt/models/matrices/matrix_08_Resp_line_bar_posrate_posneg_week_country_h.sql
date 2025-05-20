{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        test_kit IN ('adeno_pcr', 'bac_antigen', 'bac_pcr', 'covid_antigen', 'covid_pcr', 'flu_antigen', 'flu_pcr', 'test_14', 'test_21', 'test_23', 'test_24', 'test_3', 'test_4', 'thermo', 'vsr_antigen')
    GROUP BY epiweek_enddate
    ORDER BY epiweek_enddate
)

SELECT
    epiweek_enddate AS "semanas_epidemiologicas",
    MAX("posrate") * 100 AS "Positividade (%)",
    SUM("Pos") AS "Positivos",
    SUM("Neg") AS "Negativos"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    