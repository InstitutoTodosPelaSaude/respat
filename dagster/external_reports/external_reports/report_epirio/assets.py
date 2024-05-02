from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult, 
    MetadataValue
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model
)
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import date

from ..assets import all_external_reports
from ..utils import send_email_with_file, add_date_to_text

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

EXTERNAL_REPORTS_EPIRIO_RECIPIENTS = os.getenv('EXTERNAL_REPORTS_EPIRIO_RECIPIENTS').split(',')
EXTERNAL_REPORTS_EPIRIO_SUBJECT = os.getenv('EXTERNAL_REPORTS_EPIRIO_SUBJECT')
EXTERNAL_REPORTS_EPIRIO_BODY = os.getenv('EXTERNAL_REPORTS_EPIRIO_BODY')

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.parent.absolute()

EPIRIO_FOLDER_NAME = 'epirio'
EPIRIO_FILE_NAME = 'epirio_report_respat.tsv'

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([all_external_reports], "report_epirio_final")]
)
def report_epirio_export_to_tsv(context: AssetExecutionContext):
    # Get the file path
    file_path = ROOT_PATH / 'data' / 'external_reports' / EPIRIO_FOLDER_NAME
    os.makedirs(file_path, exist_ok=True)

    # Get the data from database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    query = f"""
        SELECT *
        FROM {DB_SCHEMA}.report_epirio_final
    """
    df = pd.read_sql(query, engine)
    engine.dispose()

    # Export the data to a TSV file
    file_name = file_path / EPIRIO_FILE_NAME
    df.to_csv(file_name, sep='\t', index=False)

    context.add_output_metadata({
        'num_rows': df.shape[0]
    })

@asset(
    compute_kind="python", 
    deps=[report_epirio_export_to_tsv]
)
def report_epirio_send_email(context: AssetExecutionContext):
    # Get the file path
    file_path = ROOT_PATH / 'data' / 'external_reports' / EPIRIO_FOLDER_NAME / EPIRIO_FILE_NAME

    # Send the email
    send_email_with_file(
        recipient_emails=EXTERNAL_REPORTS_EPIRIO_RECIPIENTS,
        subject=add_date_to_text(EXTERNAL_REPORTS_EPIRIO_SUBJECT),
        body=add_date_to_text(EXTERNAL_REPORTS_EPIRIO_BODY),
        file_paths=[file_path]
    )

    # Log
    context.log.info(f'Email "{EXTERNAL_REPORTS_EPIRIO_SUBJECT}" sent to {EXTERNAL_REPORTS_EPIRIO_RECIPIENTS}.')

