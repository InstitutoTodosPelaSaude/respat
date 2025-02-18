from dagster import AssetExecutionContext
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
from .constants import dbt_manifest_path
from textwrap import dedent
import pandas as pd
import os
import sys
import io
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem

dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)

MATRICES_FILES_FOLDER = "/data/respat/data/matrices/"

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

ROOT_PATH = pathlib.Path(__file__).parent.parent.parent.parent.absolute()
SAVE_PATH = ROOT_PATH / "data" / "matrices"

@dbt_assets(
    manifest=dbt_manifest_path,
    select='matrices',
    dagster_dbt_translator=dagster_dbt_translator
)
def respiratorios_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(
    compute_kind="python",
    deps=[
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_01_VRISP_line_posrate_direct_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_02_Resp_bar_pos_panel4_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_03_SC2_heat_posrate_week_state"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_04_SC2_heat_posrate_agegroups_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_06_Resp_line_posrate_direct_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_05_FLUA_heat_posrate_agegroups_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_07_Resp_bar_pos_panel20PLUS_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_08_Resp_line_bar_posrate_posneg_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_09_Resp_pyr_pos_agegroups_all_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_states"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_cities"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_states"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_cities"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_15_FLUB_heat_posrate_agegroups_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_21_SC2_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_22_VSR_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_23_FLUA_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_24_FLUB_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_count_by_labid_testkit_pathogen_result"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_SC2_posrate_by_epiweek_state"),
    ]
)
def export_matrices_to_xlsx(context):
    # Get file system
    file_system = FileSystem(root_path=MATRICES_FILES_FOLDER)

    # Delete all the files in the folder to avoid unnecessary files
    for file in file_system.list_files_in_relative_path(""):
        file = file.split("/")[-1] # Get the file name
        deleted = file_system.delete_file(file)

        if not deleted:
            raise Exception(f'Error deleting file {file}')
        context.log.info(f'Deleted {file}')

    # Map all the db matrix tables that need to be exported to its file name
    matrices_name_map = {
        "matrix_01_VRISP_line_posrate_direct_week_country":         "01_VRISP_line_posrate_direct_week_country",
        "matrix_02_Resp_bar_pos_panel4_week_country":               "02_Resp_bar_pos_panel4_week_country",
        "matrix_03_SC2_heat_posrate_week_state":                    "03_SC2_heat_posrate_week_state",
        "matrix_04_SC2_heat_posrate_agegroups_week_country":        "04_SC2_heat_posrate_agegroups_week_country",
        "matrix_05_FLUA_heat_posrate_agegroups_week_country":       "05_FLUA_heat_posrate_agegroups_week_country",
        "matrix_06_Resp_line_posrate_direct_week_country":          "06_Resp_line_posrate_direct_week_country",
        "matrix_07_Resp_bar_pos_panel20PLUS_week_country":          "07_Resp_bar_pos_panel20+_week_country",
        "matrix_08_Resp_line_bar_posrate_posneg_week_country":      "08_Resp_line_bar_posrate_posneg_week_country",
        "matrix_09_Resp_pyr_pos_agegroups_all_week_country":        "09_Resp_pyr_pos_agegroups_all_week_country",
        "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country":     "10_Resp_pyr_pos_agegroups_panel4_week_country",
        "matrix_13_SC2_map_pos_direct_states":                      "13_SC2_map_pos_direct_states",
        "matrix_13_SC2_map_pos_direct_cities":                      "13_SC2_map_pos_direct_cities",
        "matrix_14_FLUB_map_pos_direct_states":                     "14_FLUB_map_pos_direct_states",
        "matrix_14_FLUB_map_pos_direct_cities":                     "14_FLUB_map_pos_direct_cities",
        "matrix_15_FLUB_heat_posrate_agegroups_week_country":       "15_FLUB_heat_posrate_agegroups_week_country",
        "matrix_21_SC2_line_posrate_direct_week_country_annual":    "21_SC2_line_posrate_direct_week_country_annual",
        "matrix_22_VSR_line_posrate_direct_week_country_annual":    "22_VSR_line_posrate_direct_week_country_annual",
        "matrix_23_FLUA_line_posrate_direct_week_country_annual":   "23_FLUA_line_posrate_direct_week_country_annual",
        "matrix_24_FLUB_line_posrate_direct_week_country_annual":   "24_FLUB_line_posrate_direct_week_country_annual",
        "matrix_ALL_count_by_labid_testkit_pathogen_result":        "matrix_ALL_count_by_labid_testkit_pathogen_result",
        "matrix_SC2_posrate_by_epiweek_state":                      "matrix_SC2_posrate_by_epiweek_state",
    }

    # Get each matrix table and export it to a xlsx file
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    for matrix_name, new_name in matrices_name_map.items():
        matrix_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."{matrix_name}"', engine, dtype='str')

        excel_buffer = io.BytesIO()
        matrix_df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)

        file_system.save_content_in_file('', excel_buffer.read(), f'{new_name}.xlsx')
