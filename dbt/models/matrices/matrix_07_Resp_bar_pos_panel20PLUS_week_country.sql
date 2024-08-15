{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        test_kit IN ('test_21', 'test_24')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)

SELECT
    epiweek_enddate as "semana epidemiológica",
    SUM(CASE WHEN pathogen = 'RINO' THEN "Pos" ELSE 0 END) AS "Rinovírus",
    SUM(CASE WHEN pathogen = 'ENTERO' THEN "Pos" ELSE 0 END) AS "Enterovírus",
    SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END) AS "Influenza A",
    SUM(CASE WHEN pathogen = 'FLUB' THEN "Pos" ELSE 0 END) AS "Influenza B",
    SUM(CASE WHEN pathogen = 'ADENO' THEN "Pos" ELSE 0 END) AS "Adenovírus",
    SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) AS "SARS-CoV-2",
    SUM(CASE WHEN pathogen = 'COVS' THEN "Pos" ELSE 0 END) AS "Coronavírus sazonais",
    SUM(CASE WHEN pathogen = 'BOCA' THEN "Pos" ELSE 0 END) AS "Bocavírus",
    SUM(CASE WHEN pathogen = 'PARA' THEN "Pos" ELSE 0 END) AS "Vírus Parainfluenza",
    SUM(CASE WHEN pathogen = 'META' THEN "Pos" ELSE 0 END) AS "Metapneumovírus",
    SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END) AS "Vírus Sincicial Respiratório",
    SUM(CASE WHEN pathogen = 'BAC' THEN "Pos" ELSE 0 END) AS "Bactérias"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate