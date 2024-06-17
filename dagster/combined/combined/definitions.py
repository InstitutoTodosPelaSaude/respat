import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    respiratorios_historical_dbt_assets,
    respiratorios_combined_dbt_assets,
    combined_historical_raw,
    export_to_tsv,
    zip_exported_file
)
from .constants import dbt_project_dir
from .schedules import schedules
from .jobs import combined_all_assets_job, combined_historical_all_assets_job
from .sensors import run_combined_sensor, combined_slack_failure_sensor

defs = Definitions(
    assets=[
        respiratorios_historical_dbt_assets, 
        respiratorios_combined_dbt_assets,
        combined_historical_raw,
        export_to_tsv,
        zip_exported_file
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[combined_all_assets_job, combined_historical_all_assets_job],
    sensors=[run_combined_sensor, combined_slack_failure_sensor]
)