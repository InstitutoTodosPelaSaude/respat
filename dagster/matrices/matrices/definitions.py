import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import respiratorios_dbt_assets, generate_matrices, adapt_and_rename_matrices
from .constants import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[respiratorios_dbt_assets, generate_matrices, adapt_and_rename_matrices],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
)