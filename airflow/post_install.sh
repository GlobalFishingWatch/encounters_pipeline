#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_encounters \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    backfill="" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    encounters_table="encounters" \
    max_encounter_dist_km=0.5 \
    min_encounter_time_minutes=120 \
    neighbor_table="raw_encounters_neighbors_" \
    project_id="{{ var.value.PROJECT_ID }}" \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    raw_table="raw_encounters_" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="position_messages_" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \

echo "Installation Complete"

