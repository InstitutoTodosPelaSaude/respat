{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
       *
    FROM {{ ref("matrices_02_CUBE_pos_neg_posrate_totaltest") }}
)
SELECT
    combined.pathogen,
    combined.lab_id,
    combined.test_kit,
    combined.state_code,
    combined.country,
    combined.epiweek_enddate,
    combined.age_group,
    combined.state,
    combined_pivoted.metric,
    combined_pivoted.result
FROM
    source_data combined
CROSS JOIN LATERAL (
    VALUES
        (combined."Pos"         ,'Pos'),
        (combined."Neg"         ,'Neg'),
        (combined."NT"  ,'NT'),
        (combined."totaltests"  ,'totaltests'),
        (combined."posrate"     ,'posrate')
) AS combined_pivoted(result, metric)