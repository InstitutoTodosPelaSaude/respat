import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, sivep_raw, sivep_remove_used_files
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[respiratorios_dbt_assets, sivep_raw, sivep_remove_used_files],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)