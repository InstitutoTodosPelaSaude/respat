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
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv


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
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_count_by_labid_testkit_pathogen_result"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_SC2_posrate_by_epiweek_state"),
    ]
)
def export_matrices_to_xlsx(context):
    # Delete all the files in the folder, ignoring the .gitkeep file
    for file in os.listdir(SAVE_PATH):
        if file != ".gitkeep":
            os.remove(f'{SAVE_PATH}/{file}')
            context.log.info(f"Deleted file: {file}")

    # Map all the db matrix tables that need to be exported to its file name
    matrices_name_map = {
        "matrix_01_VRISP_line_posrate_direct_week_country":     "01_VRISP_line_posrate_direct_week_country",
        "matrix_02_Resp_bar_pos_panel4_week_country":           "02_Resp_bar_pos_panel4_week_country",
        "matrix_03_SC2_heat_posrate_week_state":                "03_SC2_heat_posrate_week_state",
        "matrix_04_SC2_heat_posrate_agegroups_week_country":    "04_SC2_heat_posrate_agegroups_week_country",
        "matrix_05_FLUA_heat_posrate_agegroups_week_country":   "05_FLUA_heat_posrate_agegroups_week_country",
        "matrix_06_Resp_line_posrate_direct_week_country":      "06_Resp_line_posrate_direct_week_country",
        "matrix_07_Resp_bar_pos_panel20PLUS_week_country":      "07_Resp_bar_pos_panel20+_week_country",
        "matrix_08_Resp_line_bar_posrate_posneg_week_country":  "08_Resp_line_bar_posrate_posneg_week_country",
        "matrix_09_Resp_pyr_pos_agegroups_all_week_country":    "09_Resp_pyr_pos_agegroups_all_week_country",
        "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country": "10_Resp_pyr_pos_agegroups_panel4_week_country",
        "matrix_13_SC2_map_pos_direct_states":                  "13_SC2_map_pos_direct_states",
        "matrix_13_SC2_map_pos_direct_cities":                  "13_SC2_map_pos_direct_cities",
        "matrix_14_FLUB_map_pos_direct_states":                 "14_FLUB_map_pos_direct_states",
        "matrix_14_FLUB_map_pos_direct_cities":                 "14_FLUB_map_pos_direct_cities",
        "matrix_15_FLUB_heat_posrate_agegroups_week_country":   "15_FLUB_heat_posrate_agegroups_week_country",
        "matrix_ALL_count_by_labid_testkit_pathogen_result":    "matrix_ALL_count_by_labid_testkit_pathogen_result",
        "matrix_SC2_posrate_by_epiweek_state":                  "matrix_SC2_posrate_by_epiweek_state",
    }

    # Get each matrix table and export it to a xlsx file
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    for matrix_name, new_name in matrices_name_map.items():
        matrix_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."{matrix_name}"', engine, dtype='str')

        matrix_df.to_excel(f'{SAVE_PATH}/{new_name}.xlsx', index=False)
