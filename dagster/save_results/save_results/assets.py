from dagster import (
    AssetExecutionContext,
    asset,
    MaterializeResult, 
    MetadataValue,
    AssetKey
)
from dagster.core.storage.pipeline_run import RunsFilter
from textwrap import dedent
import pandas as pd
import os
import sys
import pathlib
from sqlalchemy import create_engine
from dotenv import load_dotenv
from io import StringIO, BytesIO
import zipfile
from datetime import datetime, timedelta
import re

sys.path.insert(1, os.getcwd())
from filesystem.filesystem import FileSystem
from utils.epiweek import get_epiweek_str

REPORTS_FILES_FOLDER = "/data/respat/reports/"

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@asset(compute_kind="python")
def create_new_folder(context):
    file_system = FileSystem(root_path=REPORTS_FILES_FOLDER)

    # Get the last folder created
    all_folders = file_system.list_files_in_relative_path("")
    if all_folders:
        all_folders = [folder.split('/')[-2] for folder in all_folders]
        all_folders.sort()
        last_folder = all_folders[-1]
    else:
        last_folder = None

    # Get the params to build the new folder name
    date = datetime.now().strftime('%Y-%m-%d_%Hh%M')
    epiweek_number = get_epiweek_str(
        datetime_ = datetime.now() - timedelta(days=6), # Get the number from the last epiweek
        format = 'SE{EPINUM}.%Y',
        zfill = 2
    )
    version = 1

    # Change version number if there are others versions from the same epiweek
    if last_folder and epiweek_number in last_folder:
        # Get the previous version number
        pattern = r"V(\d+)"
        match = re.search(pattern, last_folder, re.IGNORECASE)
        previous_version = match.group(1) if match else None

        # Sum 1 to create the new version number
        if previous_version:
            version = int(previous_version) + 1
        
    # Create the final folder name
    folder_name = f'{date}_Respat_{epiweek_number}_V{version}'
    
    context.log.info(f'New folder name: {folder_name}')
    return MaterializeResult(
        metadata={
            "folder_name": folder_name
        }
    )

@asset(compute_kind="python", deps=[create_new_folder])
def save_combined_files(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text
    context.log.info(f'Saving combined files into {folder_name} folder')

    # Copy files from combined folder
    file_system = FileSystem(root_path='/data/respat/')
    file_system.copy_file_to_folder("data/combined/", 'combined.zip', f'reports/{folder_name}/')

@asset(compute_kind="python", deps=[create_new_folder])
def save_matrices_files(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text
    context.log.info(f'Saving matrices files into {folder_name} folder')

    # Copy files from matrices folder
    file_system = FileSystem(root_path='/data/respat/')
    matrix_files = file_system.list_files_in_relative_path('data/matrices/')
    for matrix_file in matrix_files:
        file_name = matrix_file.split('/')[-1]
        file_system.copy_file_to_folder("data/matrices/", file_name, f'reports/{folder_name}/matrices/')
        context.log.info(f"{file_name} saved successfully")

@asset(compute_kind="python", deps=[create_new_folder])
def save_external_reports_files(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text
    context.log.info(f'Saving external_reports files into {folder_name} folder')

    # Copy files from external_reports folder
    file_system = FileSystem(root_path='/data/respat/')
    report_folders = file_system.list_files_in_relative_path('data/external_reports/')
    for report_folder in report_folders:
        # Get all files from each subfolder
        report_folder = report_folder.split('/')[-2]
        reports = file_system.list_files_in_relative_path(f'data/external_reports/{report_folder}/')
        for report_file in reports:
            # Save each file in the subfolder
            file_name = report_file.split('/')[-1]
            file_system.copy_file_to_folder(f"data/external_reports/{report_folder}/", file_name, f'reports/{folder_name}/external_reports/{report_folder}/')
            context.log.info(f"{report_folder}/{file_name} saved successfully")



    






