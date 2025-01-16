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
import shutil

from .constants import dbt_manifest_path

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

HLAGYN_FILES_FOLDER = "/data/respat/data/hlagyn/"
HLAGYN_FILES_EXTENSION = '.xlsx'

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

@asset(compute_kind="python")
def hlagyn_raw(context):
    """
    Read all excel files from data/hlagyn folder and save to db
    """
    file_system = FileSystem(root_path=HLAGYN_FILES_FOLDER)
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Choose one of the files and run the process
    hlagyn_files = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HLAGYN_FILES_EXTENSION)]
    assert len(hlagyn_files) > 0, f"No files found in the folder {HLAGYN_FILES_FOLDER} with extension {HLAGYN_FILES_EXTENSION}"

    # Read the file
    file_to_get = hlagyn_files[0].split('/')[-1] # Get the file name
    hlagyn_df = pd.read_excel(file_system.get_file_content_as_io_bytes(file_to_get), dtype = str)
    hlagyn_df['file_name'] = hlagyn_files[0]
    context.log.info(f"Reading file {hlagyn_files[0]}")

    # Change 'Metodologia' column to Métodologia. Seems weird, but all the pipeline was 
    # made with 'Métodologia' and after HLAGyn fixed the column name to 'Metodologia'
    if 'Métodologia' not in hlagyn_df.columns:
        hlagyn_df.rename(columns={
                'Metodologia': 'Métodologia',
                'metodologia': 'Métodologia',
                'Matodologia': 'Métodologia',
                'Mrtodologia': 'Métodologia',
                'Mátodologia': 'Métodologia',
                'Metodlogia': 'Métodologia',
            }, inplace=True)

    # The columns are not the same for all files, so we need to check the columns
    # and add the missing ones.
    common_columns = ['Idade', 'Sexo', 'Pedido', 'Data Coleta', 'Métodologia', 'Cidade', 'UF']
    result_columns = [
        # Covid Files
        'Resultado',
        # PR4 Files
        'Vírus Influenza A', 
        'Vírus Influenza B',
        'Vírus Sincicial Respiratório A/B',
        'Coronavírus SARS-CoV-2',
        # PR24 Files
        'VIRUS_IA',
        'VIRUS_H1N1',
        'VIRUS_AH3',
        'VIRUS_B',
        'VIRUS_MH',
        'VIRUS_SA',
        'VIRUS_SB',
        'VIRUS_RH',
        'VIRUS_PH',
        'VIRUS_PH2',
        'VIRUS_PH3',
        'VIRUS_PH4',
        'VIRUS_ADE',
        'VIRUS_BOC',
        'VIRUS_229E',
        'VIRUS_HKU',
        'VIRUS_NL63',
        'VIRUS_OC43',
        'VIRUS_SARS',
        'VIRUS_COV2',
        'VIRUS_EV',
        'BACTE_BP',
        'BACTE_BPAR',
        'BACTE_MP'
        ]
    
    # Check if all common columns are in the file
    for column in common_columns:
        assert column in hlagyn_df.columns, f"Column {column} not found in the file {hlagyn_files[0]}"

    # Add missing columns to the dataframe
    for column in result_columns:
        if column not in hlagyn_df.columns:
            hlagyn_df[column] = None
    
    # Save to db
    hlagyn_df.to_sql('hlagyn_raw', engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    engine.dispose()

    n_rows = hlagyn_df.shape[0]
    context.add_output_metadata({'num_rows': n_rows})

    return MaterializeResult(
        metadata={
            "info": MetadataValue.md(dedent(f"""
            # HLAGyn Raw

            Last updated: {pd.Timestamp.now() - pd.Timedelta(hours=3)}

            Number of rows processed: {n_rows}
            """))
        }
    )

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

@dbt_assets(
    manifest=dbt_manifest_path,
    select='hlagyn',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python", 
    deps=[get_asset_key_for_model([respiratorios_dbt_assets], "hlagyn_final")]
)
def hlagyn_remove_used_files(context):
    """
    Remove the files that were used in the dbt process
    """
    raw_data_table = 'hlagyn_raw'
    file_system = FileSystem(root_path=HLAGYN_FILES_FOLDER)
    files_in_folder = [file for file in file_system.list_files_in_relative_path("") if file.endswith(HLAGYN_FILES_EXTENSION)]

    # Get the files that were used in the dbt process
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    used_files = pd.read_sql_query(f"SELECT DISTINCT file_name FROM {DB_SCHEMA}.{raw_data_table}", engine).file_name.to_list()
    engine.dispose()

    # Remove the files that were used
    path_to_move = "_out/"
    for used_file in used_files:
        if used_file in files_in_folder:
            context.log.info(f"Moving file {used_file} to {path_to_move}")
            file_system.move_file_to_folder("", used_file.split("/")[-1], path_to_move)
    
    # Log the unmoved files
    files_in_folder = file_system.list_files_in_relative_path("")
    context.log.info(f"Files that were not moved: {files_in_folder}")