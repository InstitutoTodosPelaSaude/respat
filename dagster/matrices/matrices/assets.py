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
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_SC2_posrate_by_epiweek_state"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_SC2_posrate_by_epiweek_state_filtered"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_posrate_pos_neg_by_epiweek"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_pos_by_epiweek_agegroup"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_FLUA_FLUB_SC2_VSR_pos_by_epiweek_PANEL"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_SC2_posrate_by_epiweek_agegroup"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_FLUA_posrate_by_epiweek_agegroup"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_posrate_by_epiweek_PANEL"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_ALL_pos_by_epiweek_PANEL"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_01_VRISP_line_posrate_direct_week_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_states"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_cities"),
    ]
)
def export_matrices_to_xlsx(context):
    # Map all the db matrix tables that need to be exported to its file name
    matrices_name_map = {
        "matrix_01_VRISP_line_posrate_direct_week_country": "01_VRISP_line_posrate_direct_week_country",
        "matrix_FLUA_FLUB_SC2_VSR_pos_by_epiweek_PANEL":    "02_Resp_bar_pos_panel4_week_country",
        "matrix_SC2_posrate_by_epiweek_state_filtered":     "03_SC2_heat_posrate_all_week_state",
        "matrix_SC2_posrate_by_epiweek_agegroup":           "04_SC2_heat_posrate_all_agegroups_week_country",
        "matrix_FLUA_posrate_by_epiweek_agegroup":          "05_FLUA_heat_posrate_all_agegroups_week_country",
        "matrix_ALL_posrate_by_epiweek_PANEL":              "06_Resp_line_posrate_panel_week_country",
        "matrix_ALL_pos_by_epiweek_PANEL":                  "07_Resp_bar_pos_panel_week_country",
        "matrix_ALL_posrate_pos_neg_by_epiweek":            "08_Resp_line_bar_posrate_posneg_all_week_country",
        "matrix_ALL_pos_by_epiweek_agegroup":               "09_Resp_pyr_pos_agegroups_all_week_country",
        "matrix_13_SC2_map_pos_direct_states":              "13_SC2_map_pos_direct_states",
        "matrix_13_SC2_map_pos_direct_cities":              "13_SC2_map_pos_direct_cities",
        "matrix_SC2_posrate_by_epiweek_state":              "matrix_SC2_posrate_by_epiweek_state",
    }

    # Get each matrix table and export it to a xlsx file
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    for matrix_name, new_name in matrices_name_map.items():
        matrix_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."{matrix_name}"', engine, dtype='str')

        matrix_df.to_excel(f'{SAVE_PATH}/{new_name}.xlsx', index=False)
