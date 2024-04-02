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
from dotenv import load_dotenv

from .jobs import combined_all_assets_job

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@multi_asset_sensor(
    monitored_assets=[
        AssetKey("einstein_final"), 
        AssetKey("hilab_final"), 
        AssetKey("hlagyn_final"), 
        AssetKey("sabin_final"), 
        AssetKey("fleury_final")
    ],
    job=combined_all_assets_job,
    default_status=DefaultSensorStatus.RUNNING
)
def run_combined_sensor(context: SensorEvaluationContext):
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
    
    # Check if all upstream jobs are finished (avoid running multiple times)
    upstream_jobs = [
        'einstein_all_assets_job', 
        'hilab_all_assets_job', 
        'hlagyn_all_assets_job', 
        'sabin_all_assets_job', 
        'fleury_all_assets_job',
        'combined_historical_final'
    ]
    for job in upstream_jobs:
        last_run = context.instance.get_runs(
            filters=RunsFilter(job_name=job)
        )
        last_run_status = None
        if len(last_run) > 0:
            last_run_status = last_run[0].status
        if last_run_status not in FINISHED_STATUSES and last_run_status is not None:
            return SkipReason(f"Upstream job {job} is not finished. Last run status is {last_run_status}")

    # Check if there are new files in the einstein folder and run the job if there are
    asset_events = context.latest_materialization_records_by_key()
    if any(asset_events.values()):
        context.advance_all_cursors()
        return RunRequest()
    
# Failure sensor that sends a message to slack
combined_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[combined_all_assets_job],
    slack_token=DAGSTER_SLACK_BOT_TOKEN,
    channel=DAGSTER_SLACK_BOT_CHANNEL,
    default_status=DefaultSensorStatus.RUNNING,
    text_fn = lambda context: f"COMBINED JOB FAILED: {context.failure_event.message}"
)