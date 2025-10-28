import os

from dagster import Definitions

from .assets import (
    create_new_folder,
    save_combined_files,
    save_matrices_files,
    save_external_reports_files,
    save_public_matrices,
    send_slack_new_folder_message
)
from .jobs import (
    create_new_folder_job,
    save_matrices_files_job,
    save_external_reports_files_job
)
from .sensors import (
    run_create_new_folder_sensor,
    run_save_matrices_files_sensor,
    run_save_external_reports_files_sensor,
    save_files_slack_failure_sensor
)

defs = Definitions(
    assets=[
        create_new_folder,
        save_combined_files,
        save_matrices_files,
        save_external_reports_files,
        save_public_matrices,
        send_slack_new_folder_message
    ],
    jobs=[
        create_new_folder_job,
        save_matrices_files_job,
        save_external_reports_files_job
    ],
    sensors=[
        run_create_new_folder_sensor,
        run_save_matrices_files_sensor,
        run_save_external_reports_files_sensor,
        save_files_slack_failure_sensor,
    ]
)