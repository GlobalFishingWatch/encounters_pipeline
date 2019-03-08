import posixpath as pp
from datetime import timedelta
import logging

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from pipe_tools.airflow.dataflow_operator import DataFlowDirectRunnerOperator
from pipe_tools.airflow.config import load_config
from pipe_tools.airflow.config import default_args


CONFIG = load_config('pipe_encounters')
DEFAULT_ARGS = default_args(CONFIG)


def table_sensor(dataset_id, table_id, date):
    return BigQueryTableSensor(
        task_id='source_exists',
        dataset_id=dataset_id,
        table_id='{}{}'.format(table_id, date),
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60),
        retry_exponential_backoff=False
    )


def build_dag(dag_id, schedule_interval='@daily', extra_default_args=None, extra_config=None):

    default_args = DEFAULT_ARGS.copy()
    default_args.update(extra_default_args or {})

    config = CONFIG.copy()
    config.update(extra_config or {})

    if schedule_interval == '@daily':
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
            table_id='position_messages_',
            date=source_sensor_date)

        segment_table_exists = table_sensor(
            dataset_id='{source_dataset}'.format(**config),
            table_id='segments_',
            date=source_sensor_date)

        python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

        # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
        # only '-' is allowed
        create_raw_encounters = DataFlowDirectRunnerOperator(
            task_id='create-raw-encounters',
            pool='dataflow',
            py_file=python_target,
            options=dict(
                startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                         'pipe_encounters/create-raw-encounters.log'),
                command='{docker_run} {docker_image} create_raw_encounters'.format(**config),
                project=config['project_id'],
                runner='{dataflow_runner}'.format(**config),
                start_date=start_date,
                end_date=end_date,
                max_encounter_dist_km=config['max_encounter_dist_km'],
                min_encounter_time_minutes=config['min_encounter_time_minutes'],
                source_dataset='{project_id}:{source_dataset}'.format(**config),
                raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                neighbor_table='{project_id}:{pipeline_dataset}.{neighbor_table}'.format(**config),
                temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                max_num_workers="100",
                disk_size_gb="50",
                requirements_file='./requirements.txt',
                setup_file='./setup.py'
            )
        )

        dag >> source_exists >> create_raw_encounters

        if not config.get('backfill', False):
            merge_encounters = DataFlowDirectRunnerOperator(
                task_id='merge-encounters',
                pool='dataflow',
                py_file=python_target,
                options=dict(
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_encounters/merge-encounters.log'),
                    command='{docker_run} {docker_image} merge_encounters'.format(**config),
                    project=config['project_id'],
                    runner='{dataflow_runner}'.format(**config),
                    start_date=default_args['start_date'].strftime("%Y-%m-%d"),
                    end_date=end_date,
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

            create_raw_encounters >> merge_encounters

        return dag


raw_encounters_daily_dag = build_dag('encounters_daily', '@daily')
raw_encounters_monthly_dag = build_dag('encounters_monthly', '@monthly')
