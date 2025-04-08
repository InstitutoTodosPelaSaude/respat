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
REPORTS_CURRENT_FILES_FOLDER = "/data/respat/reports/current/"

load_dotenv()
DAGSTER_SLACK_BOT_TOKEN = os.getenv('DAGSTER_SLACK_BOT_TOKEN')
DAGSTER_SLACK_BOT_CHANNEL = os.getenv('DAGSTER_SLACK_BOT_CHANNEL')

@asset(
    compute_kind="python",
    deps=[AssetKey('zip_exported_file')]
)
def create_new_folder(context):
    # First of all: Clean the REPORTS_CURRENT_FILES_FOLDER folder to avoid old files
    file_system = FileSystem(root_path=REPORTS_CURRENT_FILES_FOLDER)
    for file in file_system.list_files_in_relative_path("", recursive=True):
        deleted = file_system.delete_file(file)
        if not deleted:
            raise Exception(f'Error deleting file {file}')
        context.log.info(f'Deleted {file}')

    # Get the last folder created
    file_system = FileSystem(root_path=REPORTS_FILES_FOLDER)
    all_folders = file_system.list_files_in_relative_path("")
    if all_folders:
        # Remove 'current' folder
        all_folders = [folder.split('/')[-2] for folder in all_folders if 'current' not in folder] 
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

    for file_type in ['csv', 'xlsx']:
        # Copy files from matrices folder to the folder_name folder
        file_system = FileSystem(root_path='/data/respat/')
        matrix_files = file_system.list_files_in_relative_path(f'data/matrices/{file_type}/')
        for matrix_file in matrix_files:
            file_name = matrix_file.split('/')[-1]
            file_system.copy_file_to_folder(f"data/matrices/{file_type}/", file_name, f'reports/{folder_name}/matrices/{file_type}/')
            context.log.info(f"{file_name} saved successfully")

        context.log.info(f'Saving matrices files into current folder')

        # Copy files from matrices folder to the current folder
        file_system = FileSystem(root_path='/data/respat/')
        matrix_files = file_system.list_files_in_relative_path(f'data/matrices/{file_type}/')
        for matrix_file in matrix_files:
            file_name = matrix_file.split('/')[-1]
            file_system.copy_file_to_folder(f"data/matrices/{file_type}/", file_name, f'reports/current/matrices/{file_type}/')
            context.log.info(f"{file_name} saved successfully")

def start_with_number(s):
    return bool(re.match(r'^\d+', s))

@asset(compute_kind="python", deps=[save_matrices_files])
def save_public_matrices(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text
    folder_name = re.search(r'SE\d+\.\d+', folder_name).group()
    context.log.info(f'Saving public matrices files into {folder_name} folder')

    # Copy files from matrices folder to the folder_name folder
    data_file_system = FileSystem(root_path='/data/respat/')
    public_file_system = FileSystem(root_path='/public/respat/')
    matrix_files = data_file_system.list_files_in_relative_path(f'data/matrices/csv/')
    for matrix_file in matrix_files:
        file_name = matrix_file.split('/')[-1]
        if not start_with_number(file_name):
            # Skip files that don't start with a number
            continue

        # Copy the file to the public folder
        file = data_file_system.get_file_content_as_binary(f'data/matrices/csv/{file_name}')
        result = public_file_system.save_content_in_file(f"reports/{folder_name}/matrices/csv/", file, file_name)
        if not result:
            raise Exception(f'Error saving file {file_name} in public folder')
        context.log.info(f"{file_name} saved successfully")

    # Copy files from matrices folder to the current folder
    context.log.info(f'Saving public matrices files into current folder')
    matrix_files = data_file_system.list_files_in_relative_path(f'data/matrices/csv/')
    for matrix_file in matrix_files:
        file_name = matrix_file.split('/')[-1]
        if not start_with_number(file_name):
            # Skip files that don't start with a number
            continue

        # Copy the file to the public folder
        file = data_file_system.get_file_content_as_binary(f'data/matrices/csv/{file_name}')
        result = public_file_system.save_content_in_file(f"reports/current/matrices/csv/", file, file_name)
        if not result:
            raise Exception(f'Error saving file {file_name} in public folder')
        context.log.info(f"{file_name} saved successfully")

@asset(compute_kind="python", deps=[create_new_folder])
def save_external_reports_files(context):
    # Get the folder name from 'create_new_folder' asset
    materialization = context.instance.get_latest_materialization_event(AssetKey(["create_new_folder"])).asset_materialization
    folder_name = materialization.metadata["folder_name"].text

    # Copy files from external_reports folder to the folder_name folder
    context.log.info(f'Saving external_reports files into {folder_name} folder')

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

    # Copy files from external_reports folder to current folder
    context.log.info(f'Saving external_reports files into current folder')

    file_system = FileSystem(root_path='/data/respat/')
    report_folders = file_system.list_files_in_relative_path('data/external_reports/')
    for report_folder in report_folders:
        # Get all files from each subfolder
        report_folder = report_folder.split('/')[-2]
        reports = file_system.list_files_in_relative_path(f'data/external_reports/{report_folder}/')
        for report_file in reports:
            # Save each file in the subfolder
            file_name = report_file.split('/')[-1]
            file_system.copy_file_to_folder(f"data/external_reports/{report_folder}/", file_name, f'reports/current/external_reports/{report_folder}/')
            context.log.info(f"{report_folder}/{file_name} saved successfully")



    






