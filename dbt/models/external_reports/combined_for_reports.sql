{{ config(materialized='view') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(
                        from=ref('combined_final'), 
                        except=[
                            "sample_id", 
                            "test_id",
                            "lab_id",
                            "file_name",
                            "patient_id",
                            "qty_original_lines"
                        ]) 
%}

WITH source_data AS(
    SELECT
    {% for column_name in column_names %}
        "{{ column_name }}"
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    FROM {{ ref("combined_final") }}
)
SELECT
    *
FROM source_data