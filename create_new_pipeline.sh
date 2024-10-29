#!/bin/bash

TS=$(date +"%Y-%m-%d %H:%M:%S")

if [ -z "$1" ]; then
    echo "${TS} ERROR You must specify a lab name to create the pipeline"
    echo "Example: $0 LAB_NAME"
    exit 1
fi

LAB=$1
LAB=$(echo "$LAB" | tr '[:upper:]' '[:lower:]')
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

echo "${TS} INFO Project ${LAB} added to ${WORKSPACE_FILE}"

ASSETS_FILE="dagster/${PROJECT}/${PROJECT}/assets.py"
TEMPLATE_FILE="./templates/asset_template.py"


LABNAME=$(echo "$LAB" | tr '[:lower:]' '[:upper:]')   # LABNAME em maiúsculas
labname=$(echo "$LAB" | tr '[:upper:]' '[:lower:]')   # labname em minúsculas

# Define o caminho do arquivo de template e do arquivo de saída
TEMPLATE_FILE="./templates/asset_template.py"
OUTPUT_FILE="dagster/${PROJECT}/${PROJECT}/assets.py"

# Substitui LABNAME e labname no template e salva no arquivo de saída
sed "s/LABNAME/${LABNAME}/g; s/labname/${labname}/g" "$TEMPLATE_FILE" > "$OUTPUT_FILE"

echo "${TS} INFO Assets template $OUTPUT_FILE created for ${LABNAME}"