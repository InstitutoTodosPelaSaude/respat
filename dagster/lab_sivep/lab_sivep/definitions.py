import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, sivep_raw, export_to_csv, sivep_remove_used_files
from .constants import dbt_project_dir
from .schedules import schedules
from .sensors import new_sivep_file_sensor, sivep_slack_failure_sensor
from .jobs import sivep_all_assets_job

defs = Definitions(
    assets=[respiratorios_dbt_assets, sivep_raw, sivep_remove_used_files, export_to_csv],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[sivep_all_assets_job],
    sensors=[new_sivep_file_sensor, sivep_slack_failure_sensor],
)