import os
import sys
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

from .jobs import einstein_all_assets_job
from .assets import EINSTEIN_FILES_FOLDER, EINSTEIN_FILES_EXTENSION

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@sensor(
    job=einstein_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def new_einstein_file_sensor(context: SensorEvaluationContext):
    """
    Check if there are new files in the einstein folder and run the job if there are.
    The job will only run if the last run is finished to avoid running multiple times.
    """
    # Check if there are new files in the einstein folder
    file_system = FileSystem(root_path=EINSTEIN_FILES_FOLDER)
    files = file_system.list_files_in_relative_path("")
    valid_files = [file for file in files if file.endswith(EINSTEIN_FILES_EXTENSION)]
    if len(valid_files) == 0:
        return

    # Get the last run status of the job
    job_to_look = 'einstein_all_assets_job'
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
        yield SkipReason(f"There are files in the einstein folder, but the job {job_to_look} is still running with status {last_run_status}. Files: {valid_files}")

# Failure sensor that sends a message to slack
einstein_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[einstein_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    blocks_fn = lambda context: [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🔴 RESPAT: Job '{context.dagster_run.job_name}' failed",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                            "type": "plain_text",
                            "text": f"{context.failure_event.message}"
                    }
                }
            ]
)