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
from io import StringIO

from .constants import dbt_manifest_path

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
HISTORICAL_COMBINED_FILE_FOLDER = ROOT_PATH / "data" / "historical_data"
HISTORICAL_COMBINED_FILE_EXTENSION = '.tsv'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')
COMBINED_EXPORT_START_DATE = '2021-10-01'


@asset(compute_kind="python")
def combined_historical_raw(context):
    """
    Import the combined historical data to the database
    """
    
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    combined_files = [file for file in os.listdir(HISTORICAL_COMBINED_FILE_FOLDER) if file.endswith(HISTORICAL_COMBINED_FILE_EXTENSION)]
    assert len(combined_files) > 0, f"No files found in the folder {HISTORICAL_COMBINED_FILE_FOLDER} with extension {HISTORICAL_COMBINED_FILE_EXTENSION}"

    combined_file = combined_files[0]
    file_path = HISTORICAL_COMBINED_FILE_FOLDER / combined_file

    # Get a sample of data to retrieve the column names
    sample_chunk = pd.read_csv(file_path, chunksize=1, sep='\t', dtype=str)
    combined_df = next(sample_chunk)
    columns = ', '.join([f'"{col}" TEXT' for col in combined_df.columns])

    cursor = engine.raw_connection().cursor()
    # drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.combined_historical_raw")
    cursor.execute(f"CREATE TABLE {DB_SCHEMA}.combined_historical_raw ({columns})")

    # Process the data by chunks of 1,000,000 rows
    total_rows = 0
    chunk_size = 1_000_000
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, sep='\t', dtype=str):
        total_rows += len(chunk)
        chunk_buffer = StringIO()
        chunk.to_csv(chunk_buffer, index=False, header=False)
        chunk_buffer.seek(0)
    
        cursor.copy_expert(f"COPY {DB_SCHEMA}.combined_historical_raw FROM STDIN WITH CSV", chunk_buffer)
        cursor.connection.commit()


    cursor.close()
    context.add_output_metadata({'num_rows': total_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # Import Combined Historical Data

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {total_rows}
            """))
        }
    )

@dbt_assets(
    manifest=dbt_manifest_path,
    select='combined +epiweeks +municipios +age_groups +fix_location +fix_state +macroregions',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_combined_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(
    manifest=dbt_manifest_path,
    select='combined_historical',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_historical_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_combined_dbt_assets], "combined_final")]
)
def export_to_tsv(context):
    """
    Get the final combined data from the database and export to tsv
    """
    # Create data folder if not exists
    pathlib.Path('data/combined').mkdir(parents=True, exist_ok=True)

    # Export to xlsx
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    df_generator = pd.read_sql(f'select * from {DB_SCHEMA}."combined_final"', engine, chunksize=200_000)
    df = pd.concat(df_generator, ignore_index=True)

    # Drop created_at, updated_at, file_name columns
    df.drop(columns=['created_at', 'updated_at', 'file_name'], inplace=True)

    df.to_csv('data/combined/combined.tsv', sep='\t', index=False)
    engine.dispose()

    context.add_output_metadata({
        'num_rows': df.shape[0]
    })