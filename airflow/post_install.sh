#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_encounters \
    backfill="" \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_disk_size_gb="50" \
    dataflow_max_num_workers="100" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    encounters_table="encounters" \
    max_encounter_dist_km=0.5 \
    min_encounter_time_minutes=120 \
    neighbor_table="raw_encounters_neighbors_" \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    position_messages_table="position_messages_" \
    project_id="{{ var.value.PROJECT_ID }}" \
    raw_table="raw_encounters_" \
    segment_info="segment_info" \
    spatial_measures_source="pipe_static.spatial_measures_20200311" \
    distance_from_port_source="pipe_static.distance_from_port_20200311" \
    source_dataset="{{ var.value.PIPELINE_DATASET }}" \
    source_tables="position_messages_" \
    temp_bucket="{{ var.value.TEMP_BUCKET }}"

echo "Installation Complete"

