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
from sqlalchemy import create_engine
from dotenv import load_dotenv
import shutil

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
HILAB_FILES_FOLDER = ROOT_PATH / "data" / "hilab"
HILAB_FILES_EXTENSION = '.csv'

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
def hilab_raw(context):
    """
    Table with the raw data from Hilab
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    hilab_files = [file for file in os.listdir(HILAB_FILES_FOLDER) if file.endswith(HILAB_FILES_EXTENSION)]
    assert len(hilab_files) > 0, f"No files found in the folder {HILAB_FILES_FOLDER} with extension {HILAB_FILES_EXTENSION}"

    hilab_df = pd.read_csv(HILAB_FILES_FOLDER / hilab_files[0])
    hilab_df['file_name'] = hilab_files[0]
    context.log.info(f"Reading file {hilab_files[0]}")
        
    # Save to db
    hilab_df.to_sql('hilab_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = hilab_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})
    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # DBmol Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='hilab',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "hilab_final")]
)
def hilab_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'hilab_raw'
    files_in_folder = [file for file in os.listdir(HILAB_FILES_FOLDER) if file.endswith(HILAB_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM {DB_SCHEMA}.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = HILAB_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(HILAB_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(HILAB_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")

