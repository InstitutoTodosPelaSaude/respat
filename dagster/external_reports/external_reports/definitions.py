import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import all_external_reports
from .constants import dbt_project_dir

from .report_epirio.assets import (
    report_epirio_export_to_tsv,
    report_epirio_send_email
)
from .report_epirio.jobs import report_epirio_assets_job, send_report_epirio_email_job
from .report_epirio.schedules import email_report_epirio_schedule
from .report_epirio.sensors import run_report_epirio_sensor, report_epirio_slack_failure_sensor

defs = Definitions(
    assets=[
        all_external_reports,

        # EPIRIO
        report_epirio_export_to_tsv,
        report_epirio_send_email
    ],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    },
    jobs=[
        # EPIRIO
        report_epirio_assets_job,
        send_report_epirio_email_job,
    ],
    sensors=[
        # EPIRIO
        run_report_epirio_sensor,
        report_epirio_slack_failure_sensor,
    ],
    schedules=[
        # EPIRIO
        email_report_epirio_schedule,
    ]
)