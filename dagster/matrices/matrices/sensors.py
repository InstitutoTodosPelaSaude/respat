import os
from dotenv import load_dotenv

from dagster import (
    asset_sensor,
    RunRequest, 
    SkipReason, 
    SensorEvaluationContext,
    DefaultSensorStatus,
    AssetKey
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES
from dagster_slack import make_slack_on_run_failure_sensor

from .jobs import matrices_all_assets_job

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@asset_sensor(
    asset_key=AssetKey('combined_final'),
    job=matrices_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def run_matrices_sensor(context: SensorEvaluationContext):
    # Get the last run status of the job
    job_to_look = 'matrices_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # Check if the last run is finished
    if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
        return SkipReason(f"Last run status is {last_run_status}")

    return RunRequest()

# Failure sensor that sends a message to slack
matrices_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[matrices_all_assets_job],
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