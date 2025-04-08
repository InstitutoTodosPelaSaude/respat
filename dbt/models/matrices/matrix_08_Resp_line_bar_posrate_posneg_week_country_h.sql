{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
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
    