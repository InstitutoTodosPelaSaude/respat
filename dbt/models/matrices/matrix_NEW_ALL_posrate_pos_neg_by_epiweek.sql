{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        MAX("posrate") * 100 AS "posrate",
        SUM("Pos") AS "pos",
        SUM("Neg") AS "neg"
    FROM {{ ref("matrix_02_epiweek") }}
    GROUP BY epiweek_enddate
)
SELECT
    epiweek_enddate as "semanas_epidemiologicas",
    "posrate" as "Positividade (%)",
    "pos" as "Positivos",
    "neg" as  "Negativos"
FROM source_data
ORDER BY epiweek_enddate
    