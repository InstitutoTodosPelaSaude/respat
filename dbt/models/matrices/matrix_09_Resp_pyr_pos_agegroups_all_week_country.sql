{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        CASE
            WHEN "SC2_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "FLUA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "FLUB_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "VSR_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "COVS_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "ADENO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "BOCA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_24')
            WHEN "RINO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "PARA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "ENTERO_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "META_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            WHEN "BAC_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_21', 'test_24')
            ELSE FALSE
        END
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
)

SELECT
    epiweek_enddate AS "semana_epidemiológica",
    age_group AS "faixas_etárias",
    SUM(CASE WHEN pathogen = 'SC2' THEN "Pos" ELSE 0 END) AS "SARS-CoV-2",
    SUM(CASE WHEN pathogen = 'FLUA' THEN "Pos" ELSE 0 END) AS "Influenza A",
    SUM(CASE WHEN pathogen = 'FLUB' THEN "Pos" ELSE 0 END) AS "Influenza B",
    SUM(CASE WHEN pathogen = 'VSR' THEN "Pos" ELSE 0 END) AS "Vírus Sincicial Respiratório",
    SUM(CASE WHEN pathogen = 'COVS' THEN "Pos" ELSE 0 END) AS "Coronavírus sazonais",
    SUM(CASE WHEN pathogen = 'ADENO' THEN "Pos" ELSE 0 END) AS "Adenovírus",
    SUM(CASE WHEN pathogen = 'BOCA' THEN "Pos" ELSE 0 END) AS "Bocavírus",
    SUM(CASE WHEN pathogen = 'RINO' THEN "Pos" ELSE 0 END) AS "Rinovírus",
    SUM(CASE WHEN pathogen = 'PARA' THEN "Pos" ELSE 0 END) AS "Vírus Parainfluenza",
    SUM(CASE WHEN pathogen = 'ENTERO' THEN "Pos" ELSE 0 END) AS "Enterovírus",
    SUM(CASE WHEN pathogen = 'META' THEN "Pos" ELSE 0 END) AS "Metapneumovírus",
    SUM(CASE WHEN pathogen = 'BAC' THEN "Pos" ELSE 0 END) AS "Bactérias"
FROM source_data
GROUP BY epiweek_enddate, age_group
ORDER BY epiweek_enddate, age_group
    