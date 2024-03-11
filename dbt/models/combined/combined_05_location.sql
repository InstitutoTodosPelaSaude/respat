{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('combined_04_fix_location'), except=["state", "location"]) %}

WITH 
source_data AS (

    SELECT * 
    FROM {{ ref("combined_04_fix_location") }}

),

municipios AS (

    SELECT *
    FROM {{ ref("municipios") }}

),

macroregions AS (

    SELECT *
    FROM {{ ref("macroregions") }}
)

SELECT 
    {% for column_name in column_names %}
        "{{ column_name }}",
    {% endfor %}
    municipios."NM_MUN" as location,
    municipios."NM_UF" as state,
    'Brasil' as country,
    CASE 
        WHEN municipios."REGIAO" = 'NORTE' THEN 'Norte'
        WHEN municipios."REGIAO" = 'NORDESTE' THEN 'Nordeste'
        WHEN municipios."REGIAO" = 'CENTRO-OESTE' THEN 'Centro-Oeste'
        WHEN municipios."REGIAO" = 'SUDESTE' THEN 'Sudeste'
        WHEN municipios."REGIAO" = 'SUL' THEN 'Sul'
    END as region,
    macroregions."DS_NOMEPAD_macsaud" as macroregion,
    macroregions."CO_MACSAUD" as macroregion_code,
    municipios."SIGLA_UF" as state_code,
    municipios."CD_UF" as state_ibge_code,
    municipios."CD_MUN" as location_ibge_code,
    municipios.lat as lat,
    municipios.long as long
FROM source_data
LEFT JOIN municipios ON (
    source_data.location LIKE municipios."NM_MUN_NORM"
    AND source_data.state LIKE municipios."NM_UF_NORM"
)
LEFT JOIN macroregions ON (
    municipios."CD_UF" = macroregions."CO_UF"
    AND ('BR' || municipios."CD_MUN"::TEXT) = macroregions."ADM2_PCODE"
)