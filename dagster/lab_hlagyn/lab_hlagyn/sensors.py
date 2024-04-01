import os
from dotenv import load_dotenv

from dagster import (
    sensor,
    RunRequest, 
    SkipReason, 
    SensorEvaluationContext,
    DefaultSensorStatus
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES, DagsterRunStatus
from dagster_slack import make_slack_on_run_failure_sensor

from .jobs import hlagyn_all_assets_job
from .assets import ROOT_PATH, HLAGYN_FILES_FOLDER, HLAGYN_FILES_EXTENSION

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@sensor(
    job=hlagyn_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_hlagyn_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the hlagyn folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the hlagyn folder
    files = os.listdir(HLAGYN_FILES_FOLDER)
    valid_files = [file for file in files if file.endswith(HLAGYN_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'hlagyn_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # If there are no runs running, run the job
    if last_run_status in FINISHED_STATUSES or last_run_status is None:
        # Do not run if the last status is an error
        if last_run_status == DagsterRunStatus.FAILURE:
            return SkipReason(f"Last run status is an error status: {last_run_status}")
        
        yield RunRequest()
    else:
        yield SkipReason(f"There are files in the hlagyn folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
hlagyn_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[hlagyn_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    text_fn = lambda context: f"LAB JOB FAILED: {context.failure_event.message}"
)