import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import all_external_reports
from .constants import dbt_project_dir
from .schedules import schedules

from .report_epirio.assets import (
    report_epirio_export_to_tsv,
    report_epirio_send_email
)
from .report_epirio.jobs import report_epirio_assets_job

defs = Definitions(
    assets=[
        all_external_reports,

        # EPIRIO
        report_epirio_export_to_tsv,
        report_epirio_send_email
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[
        # EPIRIO
        report_epirio_assets_job,
    ],
)