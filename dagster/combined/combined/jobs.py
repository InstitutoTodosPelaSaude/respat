from dagster import define_asset_job

combined_all_assets_job = define_asset_job(
    name="combined_all_assets_job",
    selection=[
        "age_groups",
        "epiweeks",
        "fix_location",
        "macroregions",
        "municipios",
        "combined_01_join_labs",
        "combined_02_age_groups",
        "combined_03_dates",
        "combined_04_fix_location",
        "combined_05_location",
        "combined_final"
    ]
)

combined_historical_all_assets_job = define_asset_job(
    name="combined_historical_all_assets_job",
    selection=[
        "combined_historical_raw",
        "combined_historical_01_fix_types",
        "combined_historical_02_fix_values",
        "combined_historical_03_deduplicate",
        "combined_historical_final"
    ]
)