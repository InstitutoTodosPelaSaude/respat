from dagster import (
    multi_asset_sensor,
    AssetKey,
    RunRequest,
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES
from dagster_slack import make_slack_on_run_failure_sensor
import os
from dotenv import load_dotenv
from time import sleep
from datetime import datetime, time

from .jobs import combined_all_assets_job

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

# Time the sensor will wait checking if another asset started running
TIME_CHECKING_RUNNING_ASSETS = 40 # 45 seconds

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("einstein_final"), 
        AssetKey("hilab_final"), 
        AssetKey("hlagyn_final"), 
        AssetKey("sabin_final"), 
        AssetKey("fleury_final"),
        AssetKey("dbmol_final"),
        AssetKey("hpardini_final"),
        AssetKey("target_final")
    ],
    job=combined_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=180 # 3 minutes
)
def run_combined_sensor(context: SensorEvaluationContext):
    # Defining constants for the start and end datetime
    START_DAY = 1  # Tuesday
    START_TIME = time(12, 0)  # 12:00 PM (noon)
    END_DAY = 2  # Wednesday
    END_TIME = time(18, 0)  # 6:00 PM

    # Getting the current date and time
    current_datetime = datetime.now()
    current_weekday = current_datetime.weekday()
    current_time = current_datetime.time()

    # Checking if the current time is within the allowed range (Tuesday noon to Thursday 6:00 PM)
    if current_weekday < START_DAY or current_weekday > END_DAY:
        return SkipReason(f"Execution is not permitted outside of the designated window.")
    elif current_weekday == START_DAY and current_time < START_TIME:
        return SkipReason(f"Execution is not permitted outside of the designated window.")
    elif current_weekday == END_DAY and current_time > END_TIME:
        return SkipReason(f"Execution is not permitted outside of the designated window.")

    # Get the last run status of the job
    job_to_look = 'combined_all_assets_job'
    last_run = context.instance.get_runs(
        filters=RunsFilter(job_name=job_to_look)
    )
    last_run_status = None
    if len(last_run) > 0:
        last_run_status = last_run[0].status

    # Check if the last run is finished
    if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
        return SkipReason(f"Last run status is {last_run_status}")
    
    # All upstream jobs to check if they are finished
    upstream_jobs = [
        'einstein_all_assets_job', 
        'hilab_all_assets_job', 
        'hlagyn_all_assets_job', 
        'sabin_all_assets_job', 
        'fleury_all_assets_job',
        'dbmol_all_assets_job',
        'combined_historical_final',
        'hpardini_all_assets_job',
        'target_all_assets_job'
    ]

    # Check if there are new lab assets completed and run combined if it is true
    asset_events = context.latest_materialization_records_by_key()
    if any(asset_events.values()):
        # Check if all upstream jobs are finished (avoid running multiple times)
        for _ in range(0, TIME_CHECKING_RUNNING_ASSETS, 5): # Check every 5 seconds for TIME_CHECKING_RUNNING_ASSETS seconds
            sleep(5)
            for job in upstream_jobs:
                last_run = context.instance.get_runs(
                    filters=RunsFilter(job_name=job)
                )
                last_run_status = None
                if len(last_run) > 0:
                    last_run_status = last_run[0].status
                if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
                    return SkipReason(f"Upstream job {job} is not finished. Last run status is {last_run_status}")

        # If all upstream jobs are finished, return RunRequest
        context.advance_all_cursors()
        return RunRequest()
    
# Failure sensor that sends a message to slack
combined_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[combined_all_assets_job],
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