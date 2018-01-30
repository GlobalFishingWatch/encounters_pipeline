#!/usr/bin/env bash

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_encounters \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"  \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    encounters_start_date=$(date --date="7 days ago" +"%Y-%m-%d") \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_table="position_messages_" \
    raw_table="raw_encounters_" \
    encounters_table="encounters" \
    neighbor_table="raw_encounters_neighbors_"

echo "Installation Complete"

