from dagster import AssetExecutionContext
from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult, 
    MetadataValue,
    AssetKey
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
import requests
import numpy as np

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem
from utils.epiweek import get_epiweek_str

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
REPORT_AUTOMATION_WEBHOOK_URL = os.getenv('REPORT_AUTOMATION_WEBHOOK_URL')
REPORT_AUTOMATION_API_KEY = os.getenv('REPORT_AUTOMATION_API_KEY')

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
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_01_VRISP_line_posrate_direct_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_01_VRISP_line_posrate_direct_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_01_VRISP_line_posrate_direct_week_country_c"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_02_Resp_bar_pos_panel4_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_02_Resp_bar_pos_panel4_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_03_SC2_heat_posrate_week_state"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_03_SC2_heat_posrate_week_state_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_03_SC2_heat_posrate_week_state_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_04_SC2_heat_posrate_agegroups_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_04_SC2_heat_posrate_agegroups_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_05_FLUA_heat_posrate_agegroups_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_05_FLUA_heat_posrate_agegroups_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_06_Resp_line_posrate_direct_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_06_Resp_line_posrate_direct_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_07_Resp_bar_pos_panel20PLUS_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_07_Resp_bar_pos_panel20PLUS_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_08_Resp_line_bar_posrate_posneg_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_08_Resp_line_bar_posrate_posneg_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_09_Resp_pyr_pos_agegroups_all_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_09_Resp_pyr_pos_agegroups_all_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_states_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_states_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_cities_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_13_SC2_map_pos_direct_cities_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_states_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_states_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_cities_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_14_FLUB_map_pos_direct_cities_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_15_FLUB_heat_posrate_agegroups_week_country_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_15_FLUB_heat_posrate_agegroups_week_country_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_16_VSR_map_pos_direct_cities_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_16_VSR_map_pos_direct_cities_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_16_VSR_map_pos_direct_states_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_16_VSR_map_pos_direct_states_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_17_SC2_line_posrate_bar_pos_direct_week_country_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_18_VSR_line_posrate_bar_pos_direct_week_country_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_19_FLUA_line_posrate_bar_pos_direct_week_country_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_20_FLUB_line_posrate_bar_pos_direct_week_country_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_21_SC2_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_22_VSR_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_23_FLUA_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_24_FLUB_line_posrate_direct_week_country_annual"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_25_FLUA_map_pos_direct_cities_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_25_FLUA_map_pos_direct_cities_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_25_FLUA_map_pos_direct_states_h"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_25_FLUA_map_pos_direct_states_r"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_30_Resp_bar_total_posneg_months_regions"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_30_Resp_bar_total_posneg_week_regions"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_31_Resp_pyr_pos_agegroups_all_quarter_country"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_34_SC2_line_posrate_bar_pos_direct_week_regions_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_35_FLUA_line_posrate_bar_pos_direct_week_regions_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_36_FLUB_line_posrate_bar_pos_direct_week_regions_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_37_VSR_line_posrate_bar_pos_direct_week_regions_sivep"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_101_VRISP_posrate_posneg_week_region"),
        get_asset_key_for_model([respiratorios_dbt_assets], "matrix_102_VRISP_posrate_posneg_week_state"),
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
    
    for file in file_system.list_files_in_relative_path("xlsx"):
        file = file.split("/")[-1] # Get the file name
        deleted = file_system.delete_file(file)
        if not deleted:
            raise Exception(f'Error deleting file {file}')
        context.log.info(f'Deleted {file}')

    for file in file_system.list_files_in_relative_path("csv"):
        file = file.split("/")[-1]
        deleted = file_system.delete_file(file)
        if not deleted:
            raise Exception(f'Error deleting file {file}')
        context.log.info(f'Deleted {file}')

    # Map all the db matrix tables that need to be exported to its file name
    matrices_name_map = {
        "matrix_01_VRISP_line_posrate_direct_week_country_h":           "01_VRISP_line_posrate_direct_week_country_h",
        "matrix_01_VRISP_line_posrate_direct_week_country_r":           "01_VRISP_line_posrate_direct_week_country_r",
        "matrix_01_VRISP_line_posrate_direct_week_country_c":           "01_VRISP_line_posrate_direct_week_country_c",
        "matrix_02_Resp_bar_pos_panel4_week_country_h":                 "02_Resp_bar_pos_panel4_week_country_h",
        "matrix_02_Resp_bar_pos_panel4_week_country_r":                 "02_Resp_bar_pos_panel4_week_country_r",
        "matrix_03_SC2_heat_posrate_week_state":                        "03_SC2_heat_posrate_week_state",
        "matrix_03_SC2_heat_posrate_week_state_h":                      "03_SC2_heat_posrate_week_state_h",
        "matrix_03_SC2_heat_posrate_week_state_r":                      "03_SC2_heat_posrate_week_state_r",
        "matrix_04_SC2_heat_posrate_agegroups_week_country_h":          "04_SC2_heat_posrate_agegroups_week_country_h",
        "matrix_04_SC2_heat_posrate_agegroups_week_country_r":          "04_SC2_heat_posrate_agegroups_week_country_r",
        "matrix_05_FLUA_heat_posrate_agegroups_week_country_h":         "05_FLUA_heat_posrate_agegroups_week_country_h",
        "matrix_05_FLUA_heat_posrate_agegroups_week_country_r":         "05_FLUA_heat_posrate_agegroups_week_country_r",
        "matrix_06_Resp_line_posrate_direct_week_country_h":            "06_Resp_line_posrate_direct_week_country_h",
        "matrix_06_Resp_line_posrate_direct_week_country_r":            "06_Resp_line_posrate_direct_week_country_r",
        "matrix_07_Resp_bar_pos_panel20PLUS_week_country_h":            "07_Resp_bar_pos_panel20+_week_country_h",
        "matrix_07_Resp_bar_pos_panel20PLUS_week_country_r":            "07_Resp_bar_pos_panel20+_week_country_r",
        "matrix_08_Resp_line_bar_posrate_posneg_week_country_h":        "08_Resp_line_bar_posrate_posneg_week_country_h",
        "matrix_08_Resp_line_bar_posrate_posneg_week_country_r":        "08_Resp_line_bar_posrate_posneg_week_country_r",
        "matrix_09_Resp_pyr_pos_agegroups_all_week_country_h":          "09_Resp_pyr_pos_agegroups_all_week_country_h",
        "matrix_09_Resp_pyr_pos_agegroups_all_week_country_r":          "09_Resp_pyr_pos_agegroups_all_week_country_r",
        "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country_h":       "10_Resp_pyr_pos_agegroups_panel4_week_country_h",
        "matrix_10_Resp_pyr_pos_agegroups_panel4_week_country_r":       "10_Resp_pyr_pos_agegroups_panel4_week_country_r",
        "matrix_13_SC2_map_pos_direct_states_h":                        "13_SC2_map_pos_direct_states_h",
        "matrix_13_SC2_map_pos_direct_states_r":                        "13_SC2_map_pos_direct_states_r",
        "matrix_13_SC2_map_pos_direct_cities_h":                        "13_SC2_map_pos_direct_cities_h",
        "matrix_13_SC2_map_pos_direct_cities_r":                        "13_SC2_map_pos_direct_cities_r",
        "matrix_14_FLUB_map_pos_direct_states_h":                       "14_FLUB_map_pos_direct_states_h",
        "matrix_14_FLUB_map_pos_direct_states_r":                       "14_FLUB_map_pos_direct_states_r",
        "matrix_14_FLUB_map_pos_direct_cities_h":                       "14_FLUB_map_pos_direct_cities_h",
        "matrix_14_FLUB_map_pos_direct_cities_r":                       "14_FLUB_map_pos_direct_cities_r",
        "matrix_15_FLUB_heat_posrate_agegroups_week_country_h":         "15_FLUB_heat_posrate_agegroups_week_country_h",
        "matrix_15_FLUB_heat_posrate_agegroups_week_country_r":         "15_FLUB_heat_posrate_agegroups_week_country_r",
        "matrix_16_VSR_map_pos_direct_cities_h":                        "16_VSR_map_pos_direct_cities_h",
        "matrix_16_VSR_map_pos_direct_cities_r":                        "16_VSR_map_pos_direct_cities_r",
        "matrix_16_VSR_map_pos_direct_states_h":                        "16_VSR_map_pos_direct_states_h",
        "matrix_16_VSR_map_pos_direct_states_r":                        "16_VSR_map_pos_direct_states_r",
        "matrix_17_SC2_line_posrate_bar_pos_direct_week_country_sivep": "17_SC2_line_posrate_bar_pos_direct_week_country_sivep",
        "matrix_18_VSR_line_posrate_bar_pos_direct_week_country_sivep": "18_VSR_line_posrate_bar_pos_direct_week_country_sivep",
        "matrix_19_FLUA_line_posrate_bar_pos_direct_week_country_sivep":"19_FLUA_line_posrate_bar_pos_direct_week_country_sivep",
        "matrix_20_FLUB_line_posrate_bar_pos_direct_week_country_sivep":"20_FLUB_line_posrate_bar_pos_direct_week_country_sivep",
        "matrix_21_SC2_line_posrate_direct_week_country_annual":        "21_SC2_line_posrate_direct_week_country_annual",
        "matrix_22_VSR_line_posrate_direct_week_country_annual":        "22_VSR_line_posrate_direct_week_country_annual",
        "matrix_23_FLUA_line_posrate_direct_week_country_annual":       "23_FLUA_line_posrate_direct_week_country_annual",
        "matrix_24_FLUB_line_posrate_direct_week_country_annual":       "24_FLUB_line_posrate_direct_week_country_annual",
        "matrix_25_FLUA_map_pos_direct_cities_h":                       "25_FLUA_map_pos_direct_cities_h",
        "matrix_25_FLUA_map_pos_direct_cities_r":                       "25_FLUA_map_pos_direct_cities_r",
        "matrix_25_FLUA_map_pos_direct_states_h":                       "25_FLUA_map_pos_direct_states_h",
        "matrix_25_FLUA_map_pos_direct_states_r":                       "25_FLUA_map_pos_direct_states_r",
        "matrix_30_Resp_bar_total_posneg_months_regions":               "30_Resp_bar_total_posneg_months_regions",
        "matrix_30_Resp_bar_total_posneg_week_regions":                 "30_Resp_bar_total_posneg_week_regions",
        "matrix_31_Resp_pyr_pos_agegroups_all_quarter_country":         "31_Resp_pyr_pos_agegroups_all_quarter_country",
        "matrix_34_SC2_line_posrate_bar_pos_direct_week_regions_sivep": "34_SC2_line_posrate_bar_pos_direct_week_regions_sivep",
        "matrix_35_FLUA_line_posrate_bar_pos_direct_week_regions_sivep":"35_FLUA_line_posrate_bar_pos_direct_week_regions_sivep",
        "matrix_36_FLUB_line_posrate_bar_pos_direct_week_regions_sivep":"36_FLUB_line_posrate_bar_pos_direct_week_regions_sivep",
        "matrix_37_VSR_line_posrate_bar_pos_direct_week_regions_sivep": "37_VSR_line_posrate_bar_pos_direct_week_regions_sivep",
        "matrix_101_VRISP_posrate_posneg_week_region":                  "101_VRISP_posrate_posneg_week_region",
        "matrix_102_VRISP_posrate_posneg_week_state":                   "102_VRISP_posrate_posneg_week_state",
        "matrix_ALL_count_by_labid_testkit_pathogen_result":            "matrix_ALL_count_by_labid_testkit_pathogen_result",
        "matrix_SC2_posrate_by_epiweek_state":                          "matrix_SC2_posrate_by_epiweek_state",
    }

    # Get each matrix table and export it to a xlsx file and csv file
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    for matrix_name, new_name in matrices_name_map.items():
        matrix_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."{matrix_name}"', engine, dtype='str')

        # Save the xlsx file
        excel_buffer = io.BytesIO()
        matrix_df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        result = file_system.save_content_in_file('xlsx', excel_buffer.read(), f'{new_name}.xlsx', log_context=context.log)
        if not result:
            raise Exception(f'Error saving file {new_name}.xlsx')

        # Save the csv file
        csv_buffer = io.StringIO()
        matrix_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        result = file_system.save_content_in_file('csv', io.BytesIO(csv_buffer.getvalue().encode('utf-8')).read(), f'{new_name}.csv', log_context=context.log)
        if not result:
            raise Exception(f'Error saving file {new_name}.csv')

@asset(
    compute_kind="python",
    deps=[
        export_matrices_to_xlsx
    ]
)
def call_report_automation(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text

    # Create file name
    file_name = folder_name
    context.log.info(f'Creating report {file_name} on Google Drive')

    # Prepare data
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    results = dict()
    
    #### lab_data_cutoff_date
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_30_Resp_bar_total_posneg_week_regions"', engine, dtype='str')
    results['{{lab_data_cutoff_date}}'] = pd.to_datetime(temp_df["Semana Epidemiológica"]).max().strftime("%d/%m/%Y")

    #### total_results_cumulative
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_ALL_count_by_labid_testkit_pathogen_result"', engine, dtype='str')
    total = pd.to_numeric(temp_df["count"], errors="coerce").fillna(0).sum()
    results["{{total_results_cumulative}}"] = f"{int(total):,}".replace(",", ".")

    #### lab_data_cutoff_epiweek
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_30_Resp_bar_total_posneg_week_regions"', engine, dtype='str')
    results['{{lab_data_cutoff_epiweek}}'] = get_epiweek_str(pd.to_datetime(temp_df["Semana Epidemiológica"]).max().to_pydatetime(), zfill=1)

    #### top_pathogen_1_name, top_pathogen_2_name, top_pathogen_3_name, top_pathogen_4_name,
    #### top_pathogen_1_positive_share, top_pathogen_2_positive_share, top_pathogen_3_positive_share, top_pathogen_4_positive_share
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_02_Resp_bar_pos_panel4_week_country_r"', engine, dtype='str')
    row = pd.to_numeric(
        temp_df.loc[pd.to_datetime(temp_df["semana epidemiológica"]).idxmax()].drop("semana epidemiológica"),
        errors="coerce"
    ).fillna(0)
    shares = (row / row.sum() * 100) if row.sum() else (row * 0)
    top = shares.sort_values(ascending=False).head(4)
    (results['{{top_pathogen_1_name}}'], results['{{top_pathogen_2_name}}'], results['{{top_pathogen_3_name}}'], results['{{top_pathogen_4_name}}']) = top.index.tolist()
    (results['{{top_pathogen_1_positive_share}}'], results['{{top_pathogen_2_positive_share}}'], results['{{top_pathogen_3_positive_share}}'], results['{{top_pathogen_4_positive_share}}']) = np.floor(top.to_numpy() + 0.5).astype(int).tolist()

    #### sivep_data_extraction_date
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_30_Resp_bar_total_posneg_week_regions"', engine, dtype='str')
    dt = pd.to_datetime(temp_df["Semana Epidemiológica"]).max()
    results['{{sivep_data_extraction_date}}'] = f"{dt.day}/{dt.month}"

    #### others_top_positivities
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_06_Resp_line_posrate_direct_week_country_r"', engine, dtype='str')
    s = pd.to_numeric(
        temp_df.loc[pd.to_datetime(temp_df["semana epidemiológica"]).idxmax()].drop("semana epidemiológica"),
        errors="coerce"
    ).dropna()
    s = s[s > 0].sort_values(ascending=False)
    results['{{others_top_positivities}}'] = ", ".join(f"{k} ({int(np.floor(v + 0.5))}%)" for k, v in s.items())

    #### top_other_pathogen_name, top_other_pathogen_positive_share, other_pathogens_positive_shares
    temp_df = pd.read_sql_query(f'SELECT * FROM {DB_SCHEMA}."matrix_07_Resp_bar_pos_panel20PLUS_week_country_r"', engine, dtype='str')
    s = pd.to_numeric(
        temp_df.loc[pd.to_datetime(temp_df["semana epidemiológica"]).idxmax()].drop("semana epidemiológica"),
        errors="coerce"
    ).fillna(0)
    shares = ((s / s.sum() * 100) if s.sum() else (s * 0)).sort_values(ascending=False)
    shares_i = np.floor(shares.to_numpy() + 0.5).astype(int)
    results['{{top_other_pathogen_name}}'], results['{{top_other_pathogen_positive_share}}'] = shares.index[0], int(shares_i[0])
    rest = [f"{k} ({v}%)" for k, v in zip(shares.index[1:], shares_i[1:]) if v > 0]
    results['{{other_pathogens_positive_shares}}'] = ", ".join(rest[:-1]) + (" e " + rest[-1] if len(rest) > 1 else (rest[0] if rest else ""))

    # Send request
    payload = {
        "replace": [{"name": key, "value": str(value)} for key, value in results.items()]
    }
    headers = {
        "x-api-key": REPORT_AUTOMATION_API_KEY, 
        "Content-Type": "application/json",
    }
    params = {
        "file_id": file_name,
    }
    response = requests.post(
        REPORT_AUTOMATION_WEBHOOK_URL,
        headers=headers,
        params=params,
        json=payload,
        timeout=30,
    )
    response.raise_for_status()