{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        CASE
            WHEN "SC2_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "FLUA_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "FLUB_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('flu_antigen', 'flu_pcr', 'test_3', 'test_4', 'test_14', 'test_21', 'test_23', 'test_24')
            WHEN "VSR_test_result" IN ('Neg', 'Pos') THEN test_kit IN ('test_3', 'test_4', 'test_14', 'test_21', 'test_24', 'test_23', 'vsr_antigen')
            ELSE FALSE
        END
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)
SELECT
    epiweek_enddate as "semana epidemiológica",
    MAX(CASE WHEN pathogen = 'FLUA' THEN "posrate" * 100 ELSE NULL END) AS "Influenza A",
    MAX(CASE WHEN pathogen = 'FLUB' THEN "posrate" * 100 ELSE NULL END) AS "Influenza B",
    MAX(CASE WHEN pathogen = 'SC2' THEN "posrate" * 100 ELSE NULL END) AS "SARS-CoV-2",
    MAX(CASE WHEN pathogen = 'VSR' THEN "posrate" * 100 ELSE NULL END) AS "Vírus Sincicial Respiratório"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    