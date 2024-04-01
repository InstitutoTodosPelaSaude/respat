import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, einstein_raw, einstein_remove_used_files
from .constants import dbt_project_dir
from .schedules import schedules
from .sensors import new_einstein_file_sensor, einstein_slack_failure_sensor
from .jobs import einstein_all_assets_job

defs = Definitions(
    assets=[respiratorios_dbt_assets, einstein_raw, einstein_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[einstein_all_assets_job],
    sensors=[new_einstein_file_sensor, einstein_slack_failure_sensor],
)