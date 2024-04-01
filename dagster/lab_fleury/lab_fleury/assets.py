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
FLEURY_FILES_FOLDER = ROOT_PATH / "data" / "fleury"
FLEURY_FILES_EXTENSION = '.tsv'

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

@dbt_assets(
    manifest=dbt_manifest_path,
    select='fleury',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(compute_kind="python")
def fleury_raw(context):
    """
    Read excel files from data/fleury folder and save to db
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    fleury_files = [file for file in os.listdir(FLEURY_FILES_FOLDER) if file.endswith(FLEURY_FILES_EXTENSION)]
    assert len(fleury_files) > 0, f"No files found in the folder {FLEURY_FILES_FOLDER} with extension {FLEURY_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {fleury_files[0]}")
    fleury_df = pd.read_csv(FLEURY_FILES_FOLDER / fleury_files[0], encoding="latin-1", sep='\t', dtype = str)
    fleury_df['file_name'] = fleury_files[0]

    # Save to db
    fleury_df.to_sql('fleury_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = fleury_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Fleury Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "fleury_final")]
)
def fleury_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'fleury_raw'
    files_in_folder = [file for file in os.listdir(FLEURY_FILES_FOLDER) if file.endswith(FLEURY_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM {DB_SCHEMA}.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = FLEURY_FILES_FOLDER / "_out"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            shutil.move(FLEURY_FILES_FOLDER / used_file, path_to_move / used_file)
    
    # Log the unmoved files
    files_in_folder = os.listdir(FLEURY_FILES_FOLDER)
    context.log.info(f"Files that were not moved: {files_in_folder}")