import os
import posixpath as pp
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable


# The default operator doesn't template options
class TemplatedDataFlowPythonOperator(DataFlowPythonOperator):
    template_fields = ['options']

GC_CONNECTION_ID = 'google_cloud_default' 
BQ_CONNECTION_ID = 'google_cloud_default'

PROJECT_ID='{{ var.value.GCP_PROJECT_ID }}'

DATASET_ID='{{ var.value.IDENT_DATASET }}'

DOCKER_IMAGE = '{{ var.json.PIPE_ENCOUNTERS.DOCKER_IMAGE }}'

THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR

# We do this since the raw code is designed to append the date
# to random names, not necessarily after a dot. This upsets
# table_sensor. TODO: use get to grab actual table, break apart
# put together into SOURCE_DATASET and SOURCE_TABLE_PREFIX. Then
# build SOURCE_TABLE (at this time put . back on end in data)
SOURCE_TABLE = '{{ var.json.PIPE_ENCOUNTERS.SOURCE_TABLE }}'
SOURCE_TABLE_WITH_SUFFIX = '{{ var.json.PIPE_ENCOUNTERS.SOURCE_TABLE }}.'

RAW_TABLE = '{{ var.json.PIPE_ENCOUNTERS.RAW_TABLE }}'
SINK_TABLE = '{{ var.json.PIPE_ENCOUNTERS.SINK_TABLE }}'

GCP_VOLUME = '{{ var.value.GCP_VOLUME }}'

# See note about logging in readme.md
LOG_DIR = pp.join(THIS_SCRIPT_DIR, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
NORMALIZED_LOG_FILE = pp.join(LOG_DIR, 'normalized_startup.log')

TODAY_TABLE='{{ ds_nodash }}' 
YESTERDAY_TABLE='{{ yesterday_ds_nodash }}'


BUCKET='{{ var.json.PIPE_ENCOUNTERS.GCS_BUCKET }}'
GCS_TEMP_DIR='gs://%s/dataflow-temp' % BUCKET
GCS_STAGING_DIR='gs://%s/dataflow-staging' % BUCKET


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 1),
    'email': ['tim@globalfishingwatch.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': PROJECT_ID,
    'bigquery_conn_id': BQ_CONNECTION_ID,
    'gcp_conn_id': GC_CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}





@apply_defaults
def full_table (project_id, dataset_id, table_id, **kwargs):
    return '%s:%s.%s' % (project_id, dataset_id, table_id)

@apply_defaults
def table_sensor(task_id, table_id, dataset_id, dag, **kwargs):
    return BigQueryTableSensor(
        task_id=task_id,
        table_id=table_id,
        dataset_id=dataset_id,
        poke_interval=0,
        timeout=10,
        dag=dag
    )


with DAG('pipe_encounters_v0_5',  schedule_interval=timedelta(days=1), max_active_runs=3, default_args=default_args) as dag:

    yesterday_exists = table_sensor(task_id='yesterday_exists', dataset_id=SOURCE_TABLE,
                                table_id=YESTERDAY_TABLE, dag=dag)

    today_exists = table_sensor(task_id='today_exists', dataset_id=SOURCE_TABLE,
                                table_id=TODAY_TABLE, dag=dag)

    python_target = Variable.get('DATAFLOW_DOCKER_STUB')

    logging.info("target: %s", python_target)

    # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
    # only '-' is allowed
    create_raw_encounters = TemplatedDataFlowPythonOperator(
        task_id='create-raw-encounters',
        py_file=python_target,
        options={
            'startup_log_path': NORMALIZED_LOG_FILE,
            'docker_image': DOCKER_IMAGE,
            'gcp_volume': GCP_VOLUME,
            'python_module': 'pipeline.create_raw_encounters',
            'project': PROJECT_ID,
            'start_date': '{{ ds }}',
            'end_date': '{{ ds }}',
            'source_table': SOURCE_TABLE_WITH_SUFFIX,
            'raw_table': RAW_TABLE,
            'staging_location': GCS_STAGING_DIR,
            'temp_location': GCS_TEMP_DIR,
            'max_num_workers': '100',
            'disk_size_gb': '50',
            'setup_file': './setup.py',
            'requirements_file': 'requirements.txt',
        },
        dag=dag
    )


    merge_encounters = TemplatedDataFlowPythonOperator(
        task_id='merge-encounters',
        py_file=python_target,
        options={
            'startup_log_path': NORMALIZED_LOG_FILE,
            'docker_image': DOCKER_IMAGE,
            'gcp_volume': GCP_VOLUME,
            'python_module': 'pipeline.create_raw_encounters',
            'project': PROJECT_ID,
            'start_date': '{{ ds }}',
            'end_date': '{{ ds }}',
            'raw_table': RAW_TABLE,
            'sink': SINK_TABLE,
            'staging_location': GCS_STAGING_DIR,
            'temp_location': GCS_TEMP_DIR,
            'max_num_workers': '100',
            'disk_size_gb': '50',
            'setup_file': './setup.py',
            'requirements_file': 'requirements.txt',
        },
        dag=dag
    )

    yesterday_exists >> today_exists >> create_raw_encounters >> merge_encounters

