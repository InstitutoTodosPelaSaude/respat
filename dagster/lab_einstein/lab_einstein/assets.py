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
    DagsterDbtTranslatorSettings
)
from textwrap import dedent
import pandas as pd
import os
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
EINSTEIN_FILES_FOLDER = ROOT_PATH / "data" / "einstein"
EINSTEIN_FILES_EXTENSION = '.xlsx'

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
def einstein_raw(context):
    """
    Read all excel files from data/einstein folder and save to db
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    einstein_files = [file for file in os.listdir(EINSTEIN_FILES_FOLDER) if file.endswith(EINSTEIN_FILES_EXTENSION)]
    assert len(einstein_files) > 0, f"No files found in the folder {EINSTEIN_FILES_FOLDER} with extension {EINSTEIN_FILES_EXTENSION}"

    # Read the sheets 'itps_covid', 'itps_influenza' and 'itps_vsr' from the file
    einstein_df = pd.read_excel(EINSTEIN_FILES_FOLDER / einstein_files[0], dtype = str, sheet_name=['itps_covid', 'itps_influenza', 'itps_vsr'])
    einstein_df = pd.concat(einstein_df, axis=0, ignore_index=True)
    einstein_df['file_name'] = einstein_files[0]
    context.log.info(f"Reading file {einstein_files[0]}")

    # Get only the date on 'dh_coleta' column and format it to 'dd/mm/yyyy' 
    try: # Try to get the date in the format 'dd/mm/yyyy'
        einstein_df['dh_coleta'] = pd.to_datetime(einstein_df['dh_coleta'], format='%d/%m/%Y').dt.strftime('%d/%m/%Y')
    except: # If it's not in the format 'dd/mm/yyyy', try to get the date in the format 'yyyy-mm-dd'
        einstein_df['dh_coleta'] = pd.to_datetime(einstein_df['dh_coleta'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%d/%m/%Y')
        
    # Save to db
    einstein_df.to_sql('einstein_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = einstein_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Einstein Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='einstein',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

