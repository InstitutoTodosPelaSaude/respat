{{ config(materialized='table') }}

{% set epiweek_start = '2022-03-01' %}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'sc2_antigen', 'test_14', 'test_21', 'test_24', 'test_4') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, region, pathogen
),

sivep_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined_sivep") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, region, pathogen
),

source_total AS (
   SELECT
	epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('thermo', 'covid_antigen', 'covid_pcr', 'sc2_antigen', 'test_14', 'test_21', 'test_24', 'test_4') AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, pathogen
),

source_total_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN sc.pathogen = 'SC2' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade Total"
    FROM source_total sc
    GROUP BY sc.epiweek_enddate
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        sc.region,
        MAX(CASE WHEN sc.pathogen = 'SC2' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade (%, Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate, sc.region
),

sivep_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        sc.region,
        SUM(CASE WHEN sc.pathogen = 'SC2' THEN sc."Pos" ELSE 0 END)::int AS "Infecções graves por SARS-CoV-2 (SIVEP)"
    FROM sivep_data sc
    GROUP BY sc.epiweek_enddate, sc.region
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas", stp."Semanas epidemiológicas") AS "Semanas epidemiológicas",

    MAX(stp."Positividade Total") as "Brasil (Positividade)",

    MAX(CASE WHEN sp.region = 'Centro-Oeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Centro-Oeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Nordeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Nordeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Norte' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Norte (Positividade)",
    MAX(CASE WHEN sp.region = 'Sudeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Sudeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Sul' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Sul (Positividade)",

    SUM(CASE WHEN svp.region = 'Centro-Oeste' THEN "Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) as "Centro-Oeste (SRAG)",
    SUM(CASE WHEN svp.region = 'Nordeste' THEN "Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) as "Nordeste (SRAG)",
    SUM(CASE WHEN svp.region = 'Norte' THEN "Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) as "Norte (SRAG)",
    SUM(CASE WHEN svp.region = 'Sudeste' THEN "Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) as "Sudeste (SRAG)",
    SUM(CASE WHEN svp.region = 'Sul' THEN "Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) as "Sul (SRAG)"
FROM source_posrate sp
FULL OUTER JOIN sivep_posrate svp ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas" and sp.region = svp.region
LEFT JOIN source_total_posrate stp ON sp."Semanas epidemiológicas" = stp."Semanas epidemiológicas"
GROUP BY COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas", stp."Semanas epidemiológicas")
ORDER BY "Semanas epidemiológicas"

