#!/bin/bash

TS=$(date +"%Y-%m-%d %H:%M:%S")

if [ -z "$1" ]; then
    echo "${TS} ERROR You must specify a lab name to create the pipeline"
    echo "Example: $0 LAB_NAME"
    exit 1
fi

LAB=$1
PROJECT=lab_${LAB}
WORKSPACE_FILE="workspace.yaml"

# ==================================
# CREATING PROJECT BASIC STRUCTURE
# ==================================

echo "${TS} INFO Starting dagster container"
docker compose start dagster

echo "${TS} INFO Creating new project called ${PROJECT}"
docker compose exec -it -w /usr/app/respiratorios/dagster dagster dagster-dbt project scaffold --project-name lab_${LAB} --dbt-project-dir ../dbt

echo "${TS} INFO Change folder permissions to user ${USER} ${PROJECT}"
sudo chown  -R ${USER} ./dagster/
sudo chgrp  -R ${USER} ./dagster/

echo "" >> "$WORKSPACE_FILE"
echo "    - python_module:" >> "$WORKSPACE_FILE"
echo "        module_name: ${PROJECT}.definitions" >> "$WORKSPACE_FILE"
echo "        working_directory: dagster/${PROJECT}" >> "$WORKSPACE_FILE"

echo "${TS} INFO Project ${PROJECT} added to ${WORKSPACE_FILE}"
echo "${TS} INFO Creating project folders in /dbt"

mkdir -p ./dbt/models/${LAB}

echo "${TS} INFO Creating project folders in /data"

mkdir -p ./data/${LAB}
mkdir -p ./data/${LAB}/_out
touch ./data/${LAB}/.gitkeep
touch ./data/${LAB}/_out/.gitkeep

echo "${TS} INFO Finished creating the basic structure"
echo "${TS} INFO Creating basic code files"

# =====================================
# CREATING CODE FILES WITH BASIC CODE  
# =====================================

ASSETS_FILE="dagster/${PROJECT}/${PROJECT}/assets.py"
if [ -f "$ASSETS_FILE" ]; then
    sed -i "s/manifest=dbt_manifest_path/manifest=dbt_manifest_path, select='${LAB}'/" "$ASSETS_FILE"
    echo "${TS} INFO: Updated $ASSETS_FILE with select=${LAB}"
else
    echo "${TS} ERROR: File $ASSETS_FILE does not exist."
fi

echo "${TS} INFO: Project ${LAB} added to ${WORKSPACE_FILE}"