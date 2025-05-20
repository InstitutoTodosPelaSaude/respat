{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        CASE
            WHEN "COVS_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24')
            WHEN "ADENO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24', 'adeno_pcr')
            WHEN "BOCA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_23', 'test_24')
            WHEN "RINO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24')
            WHEN "PARA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24')
            WHEN "ENTERO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24')
            WHEN "META_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_14', 'test_21', 'test_23', 'test_24')
            WHEN "BAC_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_23', 'test_24', 'bac_pcr', 'bac_antigen')
            ELSE FALSE
        END
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)
SELECT
    epiweek_enddate as "semana epidemiológica",
    MAX(CASE WHEN pathogen = 'COVS' THEN "posrate" * 100 ELSE NULL END) AS "Coronavírus sazonais",
    MAX(CASE WHEN pathogen = 'ADENO' THEN "posrate" * 100 ELSE NULL END) AS "Adenovírus",
    MAX(CASE WHEN pathogen = 'BOCA' THEN "posrate" * 100 ELSE NULL END) AS "Bocavírus",
    MAX(CASE WHEN pathogen = 'RINO' THEN "posrate" * 100 ELSE NULL END) AS "Rinovírus",
    MAX(CASE WHEN pathogen = 'PARA' THEN "posrate" * 100 ELSE NULL END) AS "Vírus Parainfluenza",
    MAX(CASE WHEN pathogen = 'ENTERO' THEN "posrate" * 100 ELSE NULL END) AS "Enterovírus",
    MAX(CASE WHEN pathogen = 'META' THEN "posrate" * 100 ELSE NULL END) AS "Metapneumovírus",
    MAX(CASE WHEN pathogen = 'BAC' THEN "posrate" * 100 ELSE NULL END) AS "Bactérias"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    