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
import shutil
from io import StringIO

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

TARGET_FILES_FOLDER = "/data/respat/data/target/"
TARGET_FILES_EXTENSION = '.csv'

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
def target_raw(context):
    """
    Read excel files from data/target folder and save to db
    """
    file_system = FileSystem(root_path=TARGET_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    target_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(TARGET_FILES_EXTENSION)]
    assert len(target_files) > 0, f"No files found in the folder {TARGET_FILES_FOLDER} with extension {TARGET_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {target_files[0]}")
    file_to_get = target_files[0].split('/')[-1] # Get the file name
    target_df = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str)
    target_df['file_name'] = target_files[0]

    # Save to db
    target_df.to_sql('target_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = target_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Target Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )


@dbt_assets(manifest=dbt_manifest_path, 
            select='target', 
            dagster_dbt_translator=dagster_dbt_translator)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "target_final")]
)
def target_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'target_raw'
    file_system = FileSystem(root_path=TARGET_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(TARGET_FILES_EXTENSION)]

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

