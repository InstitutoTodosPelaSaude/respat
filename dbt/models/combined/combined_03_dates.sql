{{ config(materialized='table') }}

WITH 
source_data AS (

    SELECT * 
    FROM {{ ref("combined_02_age_groups") }}

),
epiweeks AS (
    SELECT *
    FROM {{ ref("epiweeks") }}
)
SELECT 
    source_data.*,
    ew.end_date as epiweek_enddate,
    ew.week_num as epiweek_number,

    TO_CHAR(source_data.date_testing, 'YYYY-MM') as month
    
FROM source_data
LEFT JOIN epiweeks AS ew ON source_data.date_testing >= ew.start_date AND source_data.date_testing <= ew.end_date