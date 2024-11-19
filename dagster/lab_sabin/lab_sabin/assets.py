from dagster import (
    AssetExecutionContext, 
    asset,
    sensor, 
    define_asset_job, 
    RunRequest, 
    SkipReason, 
    SensorEvaluationContext,
    DefaultSensorStatus
)
from dagster_dbt import (
    DbtCliResource, 
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    get_asset_key_for_model
)
from dagster.core.storage.pipeline_run import RunsFilter
from dagster.core.storage.dagster_run import FINISHED_STATUSES, DagsterRunStatus
from dagster_slack import make_slack_on_run_failure_sensor
import pandas as pd
import os
import sys
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import requests
import shutil

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

SABIN_FILES_FOLDER = "/data/respat/data/sabin/"
SABIN_RAW_FILES_EXTENSION = '.xlsx'
SABIN_CONVERTED_FILES_EXTENSION = '.csv'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@asset(compute_kind="python")
def sabin_convert_xlsx_to_csv(context):
    file_system = FileSystem(root_path=SABIN_FILES_FOLDER)

    for file in file_system.list_files_in_relative_path(""):
        if not file.endswith(SABIN_RAW_FILES_EXTENSION):
            continue
        
        file_to_get = file.split("/")[-1] # Get the file name
        response = requests.post(
            "http://xlsx2csv:2140/convert",
            files={"file": file_system.get_file_content_as_io_bytes(file_to_get)},
        )

        if response.status_code != 200:
            raise Exception(f"Error converting file {file}")
        
        # Save the converted file
        file_binary = response.content
        file_name = file_to_get.replace(SABIN_RAW_FILES_EXTENSION, SABIN_CONVERTED_FILES_EXTENSION)
        context.log.info(f"Saving converted file {file_name}")
        success = file_system.save_content_in_file("", file_binary, file_name, context.log)
        if not success:
            raise Exception(f"Error saving converted file {file}")
        context.log.info(f"Converted file {file}")

        # Move the original file to _out folder
        file_system.move_file_to_folder("", file.split("/")[-1], '_out/')
        context.log.info(f"Moved file {file} to _out folder")


@asset(compute_kind="python", deps=[sabin_convert_xlsx_to_csv])
def sabin_raw(context):
    file_system = FileSystem(root_path=SABIN_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files to get the columns
    sabin_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(SABIN_CONVERTED_FILES_EXTENSION)]
    assert len(sabin_files) > 0, f"No files found in {SABIN_FILES_FOLDER} with extension {SABIN_CONVERTED_FILES_EXTENSION}"

    file_to_get = sabin_files[0].split("/")[-1] # Get the file name
    sabin_df = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str)
    sabin_df['file_name'] = sabin_files[0]
    context.log.info(f"Reading file {sabin_files[0]}")

    # Save to db
    sabin_df.to_sql('sabin_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    context.add_output_metadata({'num_rows': sabin_df.shape[0]})

@dbt_assets(
        manifest=dbt_manifest_path, 
        select='sabin',
        dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "sabin_final")]
)
def sabin_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'sabin_raw'
    file_system = FileSystem(root_path=SABIN_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(SABIN_CONVERTED_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM {DB_SCHEMA}.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = "_out/"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            file_system.move_file_to_folder("", used_file.split("/")[-1], path_to_move)
    
    # Log the unmoved files
    files_in_folder = file_system.list_files_in_relative_path("")
    context.log.info(f"Files that were not moved: {files_in_folder}")
