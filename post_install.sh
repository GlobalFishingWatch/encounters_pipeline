python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force DOCKER_IMAGE=$1 \
    PIPE_ENCOUNTERS \
    SOURCE_TABLE=pipe_test_b_ttl30.position_messages_ \
    RAW_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.new_pipeline_raw_encounters_ \
    ENCOUNTERS_START_DATE=$(date --date="7 days ago" +"%Y-%m-%d") \
    SINK_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.new_pipeline_encounters_ \
    GCS_BUCKET=machine-learning-dev-ttl-30d/encounters

echo "Installation Complete"
