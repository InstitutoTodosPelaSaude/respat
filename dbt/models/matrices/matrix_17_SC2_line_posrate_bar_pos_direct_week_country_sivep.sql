{{ config(materialized='table') }}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'sc2_antigen', 'test_14', 'test_21', 'test_24', 'test_4')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
),

sivep_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined_sivep") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN sc.pathogen = 'SC2' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade (%, Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate
    ORDER BY sc.epiweek_enddate
),

sivep_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        SUM(CASE WHEN sc.pathogen = 'SC2' THEN sc."Pos" ELSE 0 END)::int AS "Infecções graves por SARS-CoV-2 (SIVEP)"
    FROM sivep_data sc
    GROUP BY sc.epiweek_enddate
    ORDER BY sc.epiweek_enddate
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas") AS "Semanas epidemiológicas",
    sp."Positividade (%, Lab. parceiros)",
    svp."Infecções graves por SARS-CoV-2 (SIVEP)"
FROM source_posrate sp
FULL OUTER JOIN sivep_posrate svp
ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas"
ORDER BY "Semanas epidemiológicas"

