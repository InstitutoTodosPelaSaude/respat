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
from io import StringIO, BytesIO

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

SIVEP_FILES_FOLDER = "/data/respat/data/SIVEP/"
INFODENGUE_FILES_PUBLIC_FOLDER = "/public/data/respat/SIVEP"
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
        "NM_UN_INTE", "CS_SEXO", "NU_IDADE_N", "TP_IDADE", "DT_COLETA",  "SEM_PRI", "AMOSTRA",
        "ID_MN_RESI",  "ID_PAIS", "ID_RG_RESI",  "SG_UF",  "CO_MUN_RES", 
        "DT_NOTIFIC", "DT_SIN_PRI", "DT_RES_AN", 
        "RES_AN", "TP_FLU_AN", "POS_AN_FLU", "POS_AN_OUT", "AN_SARS2", "AN_VSR", 
        "AN_PARA1", "AN_PARA2", "AN_PARA3", "AN_ADENO", "AN_OUTRO", 
        "DS_AN_OUT", "DT_PCR", "PCR_RESUL", "POS_PCRFLU", "POS_PCROUT", "TP_FLU_PCR", 
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

        chunk = chunk[sivep_df_columns]
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


@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "sivep_final")]
)
def sivep_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """

    context.log.info("Removing last successfully ingested file")

    raw_data_table = 'sivep_raw'
    file_system = FileSystem(root_path=SIVEP_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(SIVEP_FILES_EXTENSION)]

    context.log.info(f"Querying database to retrieve last used fil. Table {DB_SCHEMA}.{raw_data_table}")
    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM {DB_SCHEMA}.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    context.log.info(f"Moving files to _out/")

    # Remove the files that were used
    path_to_move = "_out/"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            file_system.move_file_to_folder("", used_file.split("/")[-1], path_to_move)
    
    # Log the unmoved files
    files_in_folder = file_system.list_files_in_relative_path("")
    context.log.info(f"Files that were not moved: {files_in_folder}")

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "sivep_final")]
)
def export_to_csv(context):
    """
    Get the final sivep data from the database and export to csv
    """
    file_system = FileSystem(root_path=INFODENGUE_FILES_PUBLIC_FOLDER)

    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    cursor = engine.raw_connection().cursor()


    buffer = StringIO()
    cursor.copy_expert(f'COPY (SELECT * FROM {DB_SCHEMA}."sivep_final") TO STDOUT WITH CSV DELIMITER \';\' HEADER', buffer)
    buffer.seek(0)

    file_system.save_content_in_file('', BytesIO(buffer.getvalue().encode('utf-8')).read(), 'SIVEP.csv')

    engine.dispose()
