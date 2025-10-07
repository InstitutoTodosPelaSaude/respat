{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-01' %}
{% set region = 'Sul' %}
{% set states = dbt_utils.get_column_values(
    table=ref('matrices_01_unpivot_combined_sivep'),
    column='state',
    where="region = '" ~ region ~ "' AND epiweek_enddate >= '" ~ epiweek_start ~ "'"
) 
   | reject('equalto', None) 
   | list
%}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg')
        AND test_kit IN ('thermo','covid_antigen','covid_pcr','sc2_antigen','test_14','test_21','test_24','test_4')
        AND epiweek_enddate >= '{{ epiweek_start }}'
        AND region = '{{ region }}'
    GROUP BY epiweek_enddate, region, pathogen
),

sivep_data AS (
    SELECT
        epiweek_enddate,
        state,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrices_01_unpivot_combined_sivep") }}
    WHERE
        "SC2_test_result" IN ('Pos', 'Neg')
        AND epiweek_enddate >= '{{ epiweek_start }}'
        AND region = '{{ region }}'
    GROUP BY epiweek_enddate, state, pathogen
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate AS "Semanas epidemiológicas",
        sc.region,
        MAX(CASE WHEN sc.pathogen = 'SC2' THEN sc."posrate" * 100 END) AS "Positividade (%, Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate, sc.region
),

sivep_posrate AS (
    SELECT
        sc.epiweek_enddate AS "Semanas epidemiológicas",
        sc.state,
        SUM(CASE WHEN sc.pathogen = 'SC2' THEN sc."Pos" ELSE 0 END)::int AS "Infecções graves por SARS-CoV-2 (SIVEP)"
    FROM sivep_data sc
    GROUP BY sc.epiweek_enddate, sc.state
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas") AS "Semanas epidemiológicas",

    MAX(CASE WHEN sp.region = '{{ region }}' 
             THEN "Positividade (%, Lab. parceiros)" END) AS "{{ region }} (Positividade)",

    {% for st in states %}
      SUM(CASE WHEN svp.state = '{{ st | replace("'", "''") }}' 
               THEN svp."Infecções graves por SARS-CoV-2 (SIVEP)" ELSE 0 END) 
      AS "{{ st }} (SRAG)"{{ "," if not loop.last }}
    {% endfor %}
FROM source_posrate sp
FULL OUTER JOIN sivep_posrate svp 
  ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas"
GROUP BY COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas")
ORDER BY "Semanas epidemiológicas"
