python $AIRFLOW_HOME/utils/set_default_variables.py PIPE_ANCHORAGES \
    SOURCE_TABLE=pipeline_classify_p_p516_daily. \
    RAW_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.raw_encounters_test \
    SINK_TABLE=world-fishing-827:machine_learning_dev_ttl_30d.encounters_test \
    DOCKER_IMAGE=gcr.io/world-fishing-827/$1

echo "Installation Complete"
