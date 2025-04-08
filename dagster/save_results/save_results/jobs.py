from dagster import define_asset_job

create_new_folder_job = define_asset_job(
    name="create_new_folder_job",
    selection=[
        'create_new_folder',
        'save_combined_files'
    ]
)

save_matrices_files_job = define_asset_job(
    name="save_matrices_files_job",
    selection=[
        'save_matrices_files',
        'save_public_matrices'
    ]
)

save_external_reports_files_job = define_asset_job(
    name="save_external_reports_files_job",
    selection=[
        'save_external_reports_files'
    ]
)