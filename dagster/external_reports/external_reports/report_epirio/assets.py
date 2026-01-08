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
import sys
import io
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta

from ..assets import all_external_reports
from ..utils import send_email_with_file, add_date_to_text

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem
from utils.epiweek import get_epiweek_str

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

EPIRIO_FILES_FOLDER = '/data/respat/data/external_reports/epirio/'
EPIRIO_FILE_NAME = 'report_ITpS2Epirio_respat.tsv'

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([all_external_reports], "report_epirio_final")]
)
def report_epirio_export_to_tsv(context: AssetExecutionContext):
    # Get the file system
    file_system = FileSystem(root_path=EPIRIO_FILES_FOLDER)

    # Get the data from database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    query = f"""
        SELECT *
        FROM {DB_SCHEMA}.report_epirio_final
    """
    df = pd.read_sql(query, engine)
    engine.dispose()

    # Export the data to a TSV file
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='\t', index=False)
    csv_buffer.seek(0)
    file_system.save_content_in_file('', io.BytesIO(csv_buffer.getvalue().encode('utf-8')).read(), EPIRIO_FILE_NAME)

    context.add_output_metadata({
        'num_rows': df.shape[0]
    })

@asset(
    compute_kind="python", 
    deps=[report_epirio_export_to_tsv]
)
def report_epirio_send_email(context: AssetExecutionContext):
    # Get the file path
    file_path = EPIRIO_FILES_FOLDER + EPIRIO_FILE_NAME

    # Apply mappers {date} and {epiweek} to subject and body text
    epiweek_number = get_epiweek_str(
        datetime_ = datetime.now() - timedelta(days=6), # Get the number from the last epiweek
        format = 'SE{EPINUM}_{EPIYEAR}',
        zfill = 2
    )
    subject = add_date_to_text(EXTERNAL_REPORTS_EPIRIO_SUBJECT)
    subject = subject.replace('{epiweek}', epiweek_number)
    body = add_date_to_text(EXTERNAL_REPORTS_EPIRIO_BODY)
    body = body.replace('{epiweek}', epiweek_number)

    # Create a filename with epiweek number
    file_name = os.path.basename(file_path)
    file_name = file_name.split('.')[0] + '_' + epiweek_number + '.' + file_name.split('.')[1]

    # Send the email
    send_email_with_file(
        recipient_emails=EXTERNAL_REPORTS_EPIRIO_RECIPIENTS,
        subject=subject,
        body=body,
        file_paths=[
            (file_path, file_name)
        ]
    )

    # Log
    context.log.info(f'Email "{EXTERNAL_REPORTS_EPIRIO_SUBJECT}" sent to {EXTERNAL_REPORTS_EPIRIO_RECIPIENTS}.')

