from dagster import define_asset_job

save_files_assets_job = define_asset_job(
    name="save_files_assets_job"
)