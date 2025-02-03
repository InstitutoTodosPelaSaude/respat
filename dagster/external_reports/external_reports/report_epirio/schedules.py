from dagster import ScheduleDefinition, DefaultScheduleStatus

from .jobs import send_report_epirio_email_job

email_report_epirio_schedule = ScheduleDefinition(
    job = send_report_epirio_email_job,
    cron_schedule = "0 14 * * THU",
    default_status=DefaultScheduleStatus.RUNNING
)
