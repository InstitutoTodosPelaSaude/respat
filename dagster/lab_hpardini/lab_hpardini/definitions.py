import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, hpardini_raw
from .constants import dbt_project_dir
from .schedules import schedules
from .jobs import hpardini_all_assets_job
from .sensors import new_hpardini_file_sensor, hpardini_slack_failure_sensor

defs = Definitions(
    assets=[respiratorios_dbt_assets, hpardini_raw],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[hpardini_all_assets_job],
    sensors=[new_hpardini_file_sensor, hpardini_slack_failure_sensor],
)