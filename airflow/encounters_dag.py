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


GC_CONNECTION_ID = 'google_cloud_default' 
BQ_CONNECTION_ID = 'google_cloud_default'

THIS_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FILES = THIS_SCRIPT_DIR

config = Variable.get('pipe_encounters', deserialize_json=True)
config['ds_nodash'] = '{{ ds_nodash }}'
config['first_day_of_month'] = '{{ execution_date.replace(day=1).strftime("%Y-%m-%d") }}'
config['last_day_of_month'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}'
config['first_day_of_month_nodash'] = '{{ execution_date.replace(day=1).strftime("%Y%m%d") }}'
config['last_day_of_month_nodash'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'


processing_start_date_string = config['encounters_start_date'].strip()
processing_start_date = datetime.strptime(processing_start_date_string, "%Y-%m-%d")
python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': processing_start_date,
    'email': ['tim@globalfishingwatch.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': config['project_id'],
    'dataset_id': config['pipeline_dataset'],
    'bigquery_conn_id': BQ_CONNECTION_ID,
    'gcp_conn_id': GC_CONNECTION_ID,
    'write_disposition': 'WRITE_TRUNCATE',
    'allow_large_results': True,
}

def table_sensor(dataset_id, table_id, date):
    return BigQueryTableSensor(
        task_id='source_exists',
        dataset_id=dataset_id,
        table_id='{}{}'.format(table_id, date),
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60)
    )

def build_dag(dag_id, schedule_interval):

    if schedule_interval=='@daily':
        source_sensor_date = '{{ ds_nodash }}'
        start_date = '{{ ds }}'
        end_date = '{{ ds }}'
    elif schedule_interval == '@monthly':
        source_sensor_date = '{last_day_of_month_nodash}'.format(**config)
        start_date = '{first_day_of_month}'.format(**config)
        end_date = '{last_day_of_month}'.format(**config)
    else:
        raise ValueError('Unsupported schedule interval {}'.format(schedule_interval))


    with DAG(dag_id,  schedule_interval=schedule_interval, default_args=default_args) as dag:

        source_exists = table_sensor(
            dataset_id='{source_dataset}'.format(**config),
            table_id='{source_table}'.format(**config),
            date=source_sensor_date)


        logging.info("target: %s", python_target)

        # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
        # only '-' is allowed
        create_raw_encounters = DataFlowPythonOperator(
            task_id='create-raw-encounters',
            pool='dataflow',
            py_file=python_target,
            options=dict(
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_encounters/create-raw-encounters.log'),
                command='{docker_run} {docker_image} create_raw_encounters'.format(**config),
                project=config['project_id'],
                start_date=start_date,
                end_date=end_date,
                source_table='{project_id}:{source_dataset}.{source_table}'.format(**config),
                raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                max_num_workers="100",
                disk_size_gb="50",
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        dag >> source_exists >> create_raw_encounters

        return dag

raw_encounters_daily_dag = build_dag('encounters_daily', '@daily')
raw_encounters_monthly_dag = build_dag('encounters_monthly', '@monthly')


with DAG('encounters_merge', schedule_interval='@daily', default_args=default_args) as merge_encounters_dag:
    merge_encounters = DataFlowPythonOperator(
        task_id='merge-encounters',
        pool='dataflow',
        py_file=python_target,
        options=dict(
            startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                     'pipe_encounters/merge-encounters.log'),
            command='{docker_run} {docker_image} merge_encounters'.format(**config),
            project=config['project_id'],
            start_date=processing_start_date_string,
            end_date='{{ ds }}',
            raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
            sink='{project_id}:{pipeline_dataset}.{encounters_table}'.format(**config),
            temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
            staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
            max_num_workers="100",
            disk_size_gb="50",
            requirements_file='./requirements.txt',
            setup_file='./setup.py'
        )
    )

    merge_encounters_dag >> merge_encounters