python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force DOCKER_IMAGE=$1 \
    PIPE_ENCOUNTERS \
    ENCOUNTERS_START_DATE=$(date --date="7 days ago" +"%Y-%m-%d") \
    SOURCE_TABLE="{{ PIPELINE_DATASET }}.position_messages_" \
    RAW_TABLE="{{ GCP_PROJECT_ID }}:{{ PIPELINE_DATASET }}.raw_encounters_" \
    SINK_TABLE="{{ GCP_PROJECT_ID }}:{{ PIPELINE_DATASET }}.encounters_" \

echo "Installation Complete"

