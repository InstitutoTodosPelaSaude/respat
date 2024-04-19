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
from io import StringIO

from .constants import dbt_manifest_path

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
DBMOL_FILES_FOLDER = ROOT_PATH / "data" / "dbmol"
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
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    dbmol_files = [file for file in os.listdir(DBMOL_FILES_FOLDER) if file.endswith(DBMOL_FILES_EXTENSION)]
    assert len(dbmol_files) > 0, f"No files found in the folder {DBMOL_FILES_FOLDER} with extension {DBMOL_FILES_EXTENSION}"

    # Read the sheets 'itps_covid', 'itps_influenza' and 'itps_vsr' from the file
    dbmol_df = pd.read_csv(DBMOL_FILES_FOLDER / dbmol_files[0])
    dbmol_df['file_name'] = dbmol_files[0]
    context.log.info(f"Reading file {dbmol_files[0]}")

    buffer_df = StringIO()
    # Save to db
    dbmol_df.to_csv(buffer_df, index=False, header=True)
    buffer_df.seek(0)

    cursor = engine.raw_connection().cursor()
    # drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.dbmol_raw")
    columns = ', '.join([f'"{col}" TEXT' for col in dbmol_df.columns])
    cursor.execute(f"CREATE TABLE {DB_SCHEMA}.dbmol_raw ({columns})")

    # Split the dataframe into chunks of 1,000,000 rows
    chunk_size = 1_000_000
    for i in range(0, len(dbmol_df), chunk_size):
        chunk = dbmol_df[i:i+chunk_size]
        chunk_buffer = StringIO()
        chunk.to_csv(chunk_buffer, index=False, header=False)
        chunk_buffer.seek(0)
        cursor.copy_expert(f"COPY {DB_SCHEMA}.dbmol_raw FROM STDIN WITH CSV", chunk_buffer)

    cursor.connection.commit()
    cursor.close()


    n_rows = dbmol_df.shape[0]
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
    select='dbmol',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

