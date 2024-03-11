{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('combined_03_dates'), except=["location"]) %}

WITH 
source_data AS (

    SELECT * 
    FROM {{ ref("combined_03_dates") }}

),
fix_location AS (
    SELECT *
    FROM {{ ref("fix_location") }}
)
SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    CASE
        WHEN fl.source_location IS NULL THEN source_data.location
        ELSE fl.target_location
    END AS location
FROM source_data
LEFT JOIN fix_location AS fl ON (
    source_data.location LIKE fl."source_location"
    AND source_data.state LIKE fl."source_state"
)