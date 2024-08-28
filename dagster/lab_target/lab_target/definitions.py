import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, target_raw, target_remove_used_files
from .constants import dbt_project_dir
from .schedules import schedules
from .sensors import new_target_file_sensor, target_slack_failure_sensor
from .jobs import target_all_assets_job

defs = Definitions(
    assets=[respiratorios_dbt_assets, target_raw, target_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[target_all_assets_job],
    sensors=[new_target_file_sensor, target_slack_failure_sensor],
)