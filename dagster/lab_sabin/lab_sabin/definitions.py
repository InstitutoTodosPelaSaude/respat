import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, sabin_raw, sabin_convert_xlsx_to_csv, sabin_remove_used_files
from .constants import dbt_project_dir
from .schedules import schedules
from .jobs import sabin_all_assets_job
from .sensors import new_sabin_file_sensor, sabin_slack_failure_sensor

defs = Definitions(
    assets=[respiratorios_dbt_assets, sabin_raw, sabin_convert_xlsx_to_csv, sabin_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[sabin_all_assets_job],
    sensors=[new_sabin_file_sensor, sabin_slack_failure_sensor],
)