from dagster import (
    multi_asset_sensor,
    EventLogEntry,
    asset_sensor,
    AssetKey,
    RunRequest,
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason,
    DagsterRunStatus,
    run_status_sensor
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES
from dagster_slack import (
    make_slack_on_run_failure_sensor,
    SlackResource
)
import os
from dotenv import load_dotenv
from time import sleep

from .jobs import (
    create_new_folder_job,
    save_matrices_files_job,
    save_external_reports_files_job,
)

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')
DAGSTER_SLACK_BOT_MAIN_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_MAIN_CHANNEL')
MINIO_UI_URL = os.getenv('MINIO_UI_URL')

@asset_sensor(
    asset_key=AssetKey("zip_exported_file"),
    job=create_new_folder_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30
)
def run_create_new_folder_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    return RunRequest()

@asset_sensor(
    asset_key=AssetKey("export_matrices_to_xlsx"),
    job=save_matrices_files_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30
)
def run_save_matrices_files_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    return RunRequest()

@asset_sensor(
    asset_key=AssetKey("report_epirio_export_to_tsv"),
    job=save_external_reports_files_job,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30
)
def run_save_external_reports_files_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    assert asset_event.dagster_event and asset_event.dagster_event.asset_key
    return RunRequest()    

@run_status_sensor(
    monitored_jobs=[create_new_folder_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING
)
def save_files_slack_success_sensor(context: SensorEvaluationContext):
    # Get the new report folder created by the job
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text
    context.log.info(f'Saving combined files into {folder_name} folder')

    # Minio files url
    minio_url = MINIO_UI_URL if MINIO_UI_URL.endswith('/') else MINIO_UI_URL + '/'
    minio_url = minio_url + 'browser/data/respat/'

    # Send slack report
    slack_client = SlackResource(token=DAGSTER_SLACK_BOT_TOKEN).get_client()
    slack_client.chat_postMessage(
        channel=DAGSTER_SLACK_BOT_MAIN_CHANNEL,
        blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🎉 Novo relatório do RESPAT gerado com sucesso!",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{folder_name}*",
                    }
                },
                {
                    "type": "section",
                    "text": {
                            "type": "mrkdwn",
                            "text": "O relatório foi criado e pode ser acessado pelo link abaixo:",
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "👉 *Link para o este relatório:*"
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Ir para o relatório",
                            "emoji": True
                        },
                        "value": "click_me_report",
                        "url": f"{minio_url}reports/{folder_name}/",
                        "action_id": "button-action"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Links fixos:* Os arquivos mais recentes sempre estarão disponíveis em:"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\t🔗 *Matrizes:*"
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Matrizes",
                            "emoji": True
                        },
                        "value": "click_me_matrices",
                        "url": f"{minio_url}data/matrices/",
                        "action_id": "button-action"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\t🔗 *Combined:*"
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Combined",
                            "emoji": True
                        },
                        "value": "click_me_combined",
                        "url": f"{minio_url}data/combined/",
                        "action_id": "button-action"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\t🔗 *Relatórios Externos:*"
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Relatórios Externos",
                            "emoji": True
                        },
                        "value": "click_me_ext_reports",
                        "url": f"{minio_url}data/external_reports/",
                        "action_id": "button-action"
                    }
                },
            ]
    )

# Failure sensor that sends a message to slack
save_files_slack_failure_sensor = make_slack_on_run_failure_sensor(
    monitored_jobs=[
        create_new_folder_job,
        save_matrices_files_job,
        save_external_reports_files_job,
    ],
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
