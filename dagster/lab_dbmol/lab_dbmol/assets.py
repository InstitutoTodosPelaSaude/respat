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
import pathlib
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil
from io import StringIO

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

DBMOL_FILES_FOLDER = "/data/respat/data/dbmol/"
DBMOL_FILES_EXTENSION = '.csv'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@asset(compute_kind="python")
def dbmol_raw(context):
    """
    Read all excel files from data/dbmol folder and save to db
    """
    file_system = FileSystem(root_path=DBMOL_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    dbmol_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(DBMOL_FILES_EXTENSION)]
    assert len(dbmol_files) > 0, f"No files found in the folder {DBMOL_FILES_FOLDER} with extension {DBMOL_FILES_EXTENSION}"

    dbmol_file = dbmol_files[0]
    
    # Get a sample of data to retrieve the column names
    file_to_get = dbmol_file.split('/')[-1] # Get the file name
    sample_chunk = pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), chunksize=1)
    dbmol_df = next(sample_chunk)
    columns_file = ', '.join([f'"{col}" TEXT' for col in dbmol_df.columns])
    new_column = f'"file_name" TEXT'
    columns = f"{columns_file}, {new_column}"

    cursor = engine.raw_connection().cursor()
    # drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.dbmol_raw")
    cursor.execute(f"CREATE TABLE {DB_SCHEMA}.dbmol_raw ({columns})")

    # Process the data by chunks of 1,000,000 rows
    total_rows = 0
    chunk_size = 1_000_000
    for chunk in pd.read_csv(file_system.get_file_content_as_io_bytes(file_to_get), chunksize=chunk_size):
        total_rows += len(chunk)
        chunk['file_name'] = dbmol_file
        chunk_buffer = StringIO()
        chunk.to_csv(chunk_buffer, index=False, header=False)
        chunk_buffer.seek(0)
    
        cursor.copy_expert(f"COPY {DB_SCHEMA}.dbmol_raw FROM STDIN WITH CSV", chunk_buffer)
        cursor.connection.commit()

    cursor.close()
    context.add_output_metadata({'num_rows': total_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # DBmol Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {total_rows}
            """))
        }
    )


@dbt_assets(
    manifest=dbt_manifest_path,
    select='dbmol',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "dbmol_final")]
)
def dbmol_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'dbmol_raw'
    file_system = FileSystem(root_path=DBMOL_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(DBMOL_FILES_EXTENSION)]

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