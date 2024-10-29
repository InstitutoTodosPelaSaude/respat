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

from .constants import dbt_manifest_path


ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
HPARDINI_FILES_FOLDER = ROOT_PATH / "data" / "hpardini"
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
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    hpardini_files = [file for file in os.listdir(HPARDINI_FILES_FOLDER) if file.endswith(HPARDINI_FILES_EXTENSION)]
    assert len(hpardini_files) > 0, f"No files found in the folder {HPARDINI_FILES_FOLDER} with extension {HPARDINI_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {hpardini_files[0]}")
    hpardini_df = pd.read_csv(HPARDINI_FILES_FOLDER / hpardini_files[0], dtype = str, encoding='latin-1', sep=';')
    hpardini_df['file_name'] = hpardini_files[0]

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