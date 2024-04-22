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

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()

@dbt_assets(
    manifest=dbt_manifest_path,
    select='external_reports',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "external_reports_rio_final")]
)
def external_report_rio_export_to_tsv(context: AssetExecutionContext):
    # Get the file path
    file_path = ROOT_PATH / 'data' / 'external_reports' / 'rio'
    os.makedirs(file_path, exist_ok=True)

    # Get the data from database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    query = f"""
        SELECT *
        FROM {DB_SCHEMA}.external_reports_rio_final
    """
    df = pd.read_sql(query, engine)
    engine.dispose()

    # Export the data to a TSV file
    file_name = file_path / 'external_reports_rio.tsv'
    df.to_csv(file_name, sep='\t', index=False)

    context.add_output_metadata({
        'num_rows': df.shape[0]
    })
