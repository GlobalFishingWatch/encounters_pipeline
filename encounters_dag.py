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

THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR

# We do this since the raw code is designed to append the date
# to random names, not necessarily after a dot. This upsets
# table_sensor. 
SOURCE_TABLE = '{{ var.json.PIPE_ENCOUNTERS.SOURCE_TABLE }}'
SOURCE_TABLE_WITH_SUFFIX = '{{ var.json.PIPE_ENCOUNTERS.SOURCE_TABLE }}.'

RAW_TABLE = '{{ var.json.PIPE_ENCOUNTERS.RAW_TABLE }}'
SINK_TABLE = '{{ var.json.PIPE_ENCOUNTERS.SINK_TABLE }}'

TODAY_TABLE='{{ ds_nodash }}' 
YESTERDAY_TABLE='{{ yesterday_ds_nodash }}'

FIRST_DAY_OF_MONTH = '{{ execution_date.replace(day=1).strftime("%Y-%m-%d") }}'
LAST_DAY_OF_MONTH = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}'
LAST_DAY_OF_MONTH_NODASH = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'


BUCKET='{{ var.json.PIPE_ENCOUNTERS.GCS_BUCKET }}'
GCS_TEMP_DIR='gs://%s/dataflow-temp' % BUCKET
GCS_STAGING_DIR='gs://%s/dataflow-staging' % BUCKET


processing_start_date_string = Variable.get('PIPE_ENCOUNTERS', deserialize_json=True)['ENCOUNTERS_START_DATE'].strip()
processing_start_date = datetime.strptime(processing_start_date_string, "%Y-%m-%d")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': processing_start_date,
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
        dag=dag,
        retry_delay=timedelta(minutes=60),
        retries=24*7
    )


def build_dag(dag_id, schedule_interval):

    if schedule_interval=='@daily':
        source_sensor_date = '{{ ds_nodash }}'
        start_date = '{{ ds }}'
        end_date = '{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = LAST_DAY_OF_MONTH_NODASH
        start_date = FIRST_DAY_OF_MONTH
        end_date = LAST_DAY_OF_MONTH
    else:
        raise ValueError('Unsupported schedule interval {}'.format(schedule_interval))


    with DAG(dag_id,  schedule_interval, default_args=default_args) as dag:

        source_exists = table_sensor(task_id='source_exists', dataset_id=SOURCE_TABLE,
                                    table_id=source_sensor_date, dag=dag)

        python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

        logging.info("target: %s", python_target)

        # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
        # only '-' is allowed
        create_raw_encounters = TemplatedDataFlowPythonOperator(
            task_id='create-raw-encounters',
            depends_on_past=True,
            py_file=python_target,
            options={
                'startup_log_file': pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 
                                             'pipe_encounters/create-raw-encounters.log'),
                'command': '{{ var.value.DOCKER_RUN }} {{ var.json.PIPE_ENCOUNTERS.DOCKER_IMAGE }} '
                           'python -m pipeline.create_raw_encounters',
                'project': PROJECT_ID,
                'start_date': start_date,
                'end_date': end_date,
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
                'startup_log_file': pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'), 
                                             'pipe_encounters/merge-encounters.log'),
                'command': '{{ var.value.DOCKER_RUN }} {{ var.json.PIPE_ENCOUNTERS.DOCKER_IMAGE }} '
                           'python -m pipeline.merge_encounters',
                'project': PROJECT_ID,
                'start_date': processing_start_date_string, # Run merge from first date processed
                'end_date': end_date,
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

        source_exists >> create_raw_encounters >> merge_encounters

        return dag

port_events_daily_dag = build_dag('encounters_daily_v0_14', '@daily')
port_events_monthly_dag = build_dag('encounters_monthly_v0_14', '@monthly')
