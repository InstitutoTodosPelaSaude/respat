{{ config(materialized='table') }}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "VSR_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('vsr_antigen', 'test_4', 'test_3', 'test_14', 'test_23', 'test_21', 'test_24')
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
        "VSR_test_result" IN ('Pos', 'Neg')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN sc.pathogen = 'VSR' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade (%, Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate
    ORDER BY sc.epiweek_enddate
),

sivep_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        SUM(CASE WHEN sc.pathogen = 'VSR' THEN sc."Pos" ELSE 0 END)::int AS "Infecções graves por VSR (SIVEP)"
    FROM sivep_data sc
    GROUP BY sc.epiweek_enddate
    ORDER BY sc.epiweek_enddate
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas") AS "Semanas epidemiológicas",
    sp."Positividade (%, Lab. parceiros)",
    svp."Infecções graves por VSR (SIVEP)"
FROM source_posrate sp
FULL OUTER JOIN sivep_posrate svp
ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas"
ORDER BY "Semanas epidemiológicas"

