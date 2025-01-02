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
from textwrap import dedent
import pandas as pd
import os
import sys
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

HPARDINI_FILES_FOLDER = "/data/respat/data/hpardini/"
HPARDINI_FILES_EXTENSION = '.csv'

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')


@asset(compute_kind="python")
def hpardini_raw(context):
    """
    Read excel files from data/hpardini folder and save to db
    """
    file_system = FileSystem(root_path=HPARDINI_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    hpardini_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HPARDINI_FILES_EXTENSION)]
    assert len(hpardini_files) > 0, f"No files found in the folder {HPARDINI_FILES_FOLDER} with extension {HPARDINI_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {hpardini_files[0]}")
    file_to_get = hpardini_files[0].split('/')[-1] # Get the file name
    hpardini_df = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str, encoding='latin-1', sep=';')
    hpardini_df['file_name'] = hpardini_files[0]

    # Change all dates from dd/mm/yyyy to yyyy-mm-dd. But the column values can be in both formats
    date_column = 'DATACOLETA'
    hpardini_df[date_column] = pd.to_datetime(hpardini_df[date_column], errors='coerce').dt.strftime('%Y-%m-%d')

    # Save to db
    hpardini_df.to_sql('hpardini_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = hpardini_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # hpardini Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='hpardini',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()