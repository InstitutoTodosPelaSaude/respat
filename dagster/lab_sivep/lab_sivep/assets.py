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
SIVEP_FILES_FOLDER = ROOT_PATH / "data" / "sivep"
SIVEP_FILES_EXTENSION = '.csv'

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
def sivep_raw(context):
    """
    Read excel files from data/sivep folder and save to db
    """
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    sivep_files = [file for file in os.listdir(SIVEP_FILES_FOLDER) if file.endswith(SIVEP_FILES_EXTENSION)]
    assert len(sivep_files) > 0, f"No files found in the folder {SIVEP_FILES_FOLDER} with extension {SIVEP_FILES_EXTENSION}"

    # Read the file
    context.log.info(f"Reading file {sivep_files[0]}")
    sivep_df = pd.read_csv(SIVEP_FILES_FOLDER / sivep_files[0], dtype = str)
    sivep_df['file_name'] = sivep_files[0]

    # Save to db
    sivep_df.to_sql('sivep_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = sivep_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # sivep Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='sivep',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()