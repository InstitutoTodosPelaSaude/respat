{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        CONCAT(
            'SE', 
            TO_CHAR(epiweek_number, 'fm00'), 
            ' - ', 
            {{ get_month_name_from_epiweek_number('epiweek_number') }}
        ) as epiweek_month,
        EXTRACT('Year' FROM epiweek_enddate) as epiweek_year,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE 
        "SC2_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'sc2_antigen', 'test_14', 'test_21', 'test_23', 'test_24', 'test_4')
    GROUP BY epiweek_month, epiweek_year, pathogen
    ORDER BY epiweek_month, epiweek_year, pathogen
)

SELECT
    epiweek_month as "semana epidemiológica  (SE)",
    MAX(CASE WHEN pathogen = 'SC2' AND epiweek_year = 2022 THEN "posrate" * 100 ELSE NULL END) as "2022",
    MAX(CASE WHEN pathogen = 'SC2' AND epiweek_year = 2023 THEN "posrate" * 100 ELSE NULL END) as "2023",
    MAX(CASE WHEN pathogen = 'SC2' AND epiweek_year = 2024 THEN "posrate" * 100 ELSE NULL END) as "2024",
    MAX(CASE WHEN pathogen = 'SC2' AND epiweek_year = 2025 THEN "posrate" * 100 ELSE NULL END) as "2025"
FROM source_data
GROUP BY epiweek_month
ORDER BY epiweek_month
    