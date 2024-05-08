{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('combined_03_dates'), except=["location", "state"]) %}

WITH 
source_data AS (

    SELECT * 
    FROM {{ ref("combined_03_dates") }}

),
fix_location AS (
    SELECT *
    FROM {{ ref("fix_location") }}
),
fix_state AS (
    SELECT *
    FROM {{ ref("fix_state") }}
)
SELECT
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    CASE
        WHEN fl.source_location IS NULL THEN source_data.location -- If the location is correct, keep the original value
        ELSE fl.target_location
    END AS location,
    CASE
        WHEN fs.source_state IS NULL THEN source_data.state -- If the state is correct, keep the original value
        ELSE fs.target_state
    END AS state
FROM source_data
LEFT JOIN fix_state AS fs ON source_data.state ILIKE fs."source_state"
LEFT JOIN fix_location AS fl ON (
    source_data.location ILIKE fl."source_location"
    AND (
        source_data.state ILIKE fl."source_state" -- Original state name
        OR
        fs.target_state ILIKE fl."source_state"   -- Fixed state name
    )
)