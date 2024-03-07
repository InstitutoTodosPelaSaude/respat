import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_historical_dbt_assets, combined_historical_raw
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[respiratorios_historical_dbt_assets, combined_historical_raw],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)