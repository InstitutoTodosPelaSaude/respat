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
from io import StringIO

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

SIVEP_FILES_FOLDER = "/data/respat/data/SIVEP/"
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

    file_system = FileSystem(root_path=SIVEP_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    cursor = engine.raw_connection().cursor()

    sivep_df_columns = [
        "ID_UNIDADE", "CS_SEXO", "DT_COLETA",  "SEM_PRI", "AMOSTRA",
        "ID_MN_RESI",  "ID_PAIS", "ID_RG_RESI",  "SG_UF",  "CO_MUN_RES", 
        "DT_NOTIFIC", "DT_SIN_PRI", "DT_RES_AN", 
        "RES_AN", "POS_AN_FLU", "POS_AN_OUT", "AN_SARS2", "AN_VSR", 
        "AN_PARA1", "AN_PARA2", "AN_PARA3", "AN_ADENO", "AN_OUTRO", 
        "DS_AN_OUT", "DT_PCR", "PCR_RESUL", "POS_PCRFLU", "POS_PCROUT", 
        "PCR_SARS2", "PCR_VSR", "PCR_PARA1", "PCR_PARA2", "PCR_PARA3", "PCR_PARA4", 
        "PCR_ADENO", "PCR_METAP", "PCR_BOCA", "PCR_RINO", "PCR_OUTRO", "DS_PCR_OUT", 
        "CLASSI_FIN", "CLASSI_OUT", "CRITERIO"
    ]

    # Choose one of the files and run the process
    sivep_files = [
        file for file 
        in file_system.list_files_in_relative_path("") 
        if file.endswith(SIVEP_FILES_EXTENSION)
    ]

    context.log.info(f"Found {len(sivep_files)} files in {SIVEP_FILES_FOLDER}")
    assert len(sivep_files) > 0, f"No files found in the folder {SIVEP_FILES_FOLDER} with extension {SIVEP_FILES_EXTENSION}"

    sivep_file = sivep_files[0]
    context.log.info(f"Processing {sivep_file}")

    columns = ', '.join([f'{col} TEXT' for col in sivep_df_columns])
    filename_column = f'file_name TEXT'
    columns = f"{columns}, {filename_column}"

    # drop table if exists
    context.log.info(f"Dropping table {DB_SCHEMA}.sivep_raw")
    cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.sivep_raw")
    cursor.execute(f"CREATE TABLE {DB_SCHEMA}.sivep_raw ({columns})")

    # Process the data by chunks
    file_to_get = sivep_file.split('/')[-1]
    total_rows = 0
    chunk_size = 1_000_000
    chunks_sivep_df = pd.read_csv(
        file_system.get_file_content_as_io_bytes(file_to_get), 
        chunksize=chunk_size, 
        usecols=sivep_df_columns,
        sep=';', 
        encoding='latin-1'
    )

    for i, chunk in enumerate(chunks_sivep_df):
        context.log.info(f"Chunk {i} - Start reading")

        chunk_rows = len(chunk)
        total_rows += chunk_rows
        
        context.log.info(f"Chunk {i} - Saving {chunk_rows} rows in the Buffer")

        chunk['file_name'] = sivep_file
        chunk_buffer = StringIO()
        chunk.to_csv(chunk_buffer, index=False, header=False)
        chunk_buffer.seek(0)

        context.log.info(f"Chunk {i} - Finished saving {chunk_rows} rows in the Buffer")
        context.log.info(f"Chunk {i} - Writing CSV Buffer into `{DB_SCHEMA}.sivep_raw` ")

        cursor.copy_expert(f"COPY {DB_SCHEMA}.sivep_raw FROM STDIN WITH CSV", chunk_buffer)
        context.log.info(f"Chunk {i} - Finished saving {chunk_rows} rows in table `{DB_SCHEMA}.sivep_raw` ")

        cursor.connection.commit()

    cursor.close()
    context.add_output_metadata({'num_rows': total_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # sivep Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}
            File processed: {sivep_file}
            Number of rows processed: {total_rows}
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