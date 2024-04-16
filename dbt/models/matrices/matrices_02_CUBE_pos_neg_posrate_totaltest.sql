{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        lab_id,
        state,
        state_code,
        country,
        test_kit,
        epiweek_enddate,
        result,
        age_group,
        pathogen
    FROM {{ ref("matrices_01_unpivot_combined") }}
)
SELECT
    -- Add state name to the CUBE
    t.*,
    state_table.state AS "state"
FROM (
    SELECT
        -- Create the CUBE 
        pathogen,
        lab_id,
        test_kit,
        state_code,
        country,
        epiweek_enddate,
        age_group,

        -- # Key indicators
        -- Total Number of Pos
        -- Total Number of Neg
        -- Total Number of Tests (Pos + Neg)
        -- Total Number of NT
        -- Positivity Rate (Pos/Pos+Neg)

        SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END) AS "Pos",
        SUM(CASE WHEN result = 'Neg' THEN 1 ELSE 0 END) AS "Neg",
        SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) AS "totaltests",
        SUM(CASE WHEN result = 'NT'  THEN 1 ELSE 0 END) AS "NT",
        CASE
            -- Only compute positivity rate if there are more than 50 tests
            WHEN SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END) > 50 THEN
                SUM(CASE WHEN result = 'Pos' THEN 1 ELSE 0 END)::decimal / SUM(CASE WHEN result IN ('Pos', 'Neg') THEN 1 ELSE 0 END)
            ELSE NULL
        END AS "posrate"

    FROM
        source_data
    GROUP BY
        GROUPING SETS (
            (epiweek_enddate, pathogen, country, state_code, lab_id, test_kit, age_group),
            (epiweek_enddate, pathogen),
            (epiweek_enddate, pathogen, lab_id), -- To validate the results
            (epiweek_enddate, pathogen, country, age_group),
            (epiweek_enddate, pathogen, test_kit),
            (epiweek_enddate, pathogen, country),
            (epiweek_enddate, pathogen, state_code),
            (epiweek_enddate, country)
        )
) AS t
LEFT JOIN (
    SELECT 
        state_code, state
    FROM {{ ref("matrices_01_unpivot_combined") }}
    GROUP BY state_code, state
) as state_table ON t.state_code = state_table.state_code