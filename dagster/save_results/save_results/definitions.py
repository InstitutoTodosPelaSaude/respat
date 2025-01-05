import os

from dagster import Definitions

from .assets import (
    create_new_folder,
    save_combined_files,
    save_matrices_files,
    save_external_reports_files
)
from .jobs import save_files_assets_job
from .sensors import (
    run_save_results_sensor, 
    save_files_slack_success_sensor,
    save_files_slack_failure_sensor
)

defs = Definitions(
    assets=[
        create_new_folder,
        save_combined_files,
        save_matrices_files,
        save_external_reports_files
    ],
    jobs=[save_files_assets_job],
    sensors=[
        run_save_results_sensor,
        save_files_slack_success_sensor,
        save_files_slack_failure_sensor,
    ]
)