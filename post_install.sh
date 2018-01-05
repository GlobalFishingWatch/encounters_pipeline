python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force DOCKER_IMAGE=$1 \
    PIPE_ENCOUNTERS \
    SOURCE_TABLE=pipeline_classify_p_p516_daily \
    RAW_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_ \
    SINK_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.encounters_ \
    GCS_BUCKET=machine-learning-dev-ttl-30d/encounters

echo "Installation Complete"
