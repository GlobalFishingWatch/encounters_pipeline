from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

import posixpath as pp
from datetime import timedelta


PIPELINE='pipe_encounters'


class PipeEncountersDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeEncountersDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        if schedule_interval!='@daily' and schedule_interval != '@monthly' and schedule_interval != '@yearly':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config

        start_date, end_date = self.source_date_range()

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_sensors = self.source_table_sensors(dag)

            python_target = Variable.get('DATAFLOW_WRAPPER_STUB')

            # Note: task_id must use '-' instead of '_' because it gets used to create the dataflow job name, and
            # only '-' is allowed
            create_raw_encounters = DataFlowDirectRunnerOperator(
                task_id='create-raw-encounters',
                pool='dataflow',
                py_file=python_target,
                options=dict(
                    # Airflow
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_encounters/create-raw-encounters.log'),
                    command='{docker_run} {docker_image} create_raw_encounters'.format(**config),
                    runner='{dataflow_runner}'.format(**config),

                    # Required
                    source_table='{source_dataset}.{position_messages_table}'.format(**config),
                    raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                    start_date=start_date,
                    end_date=end_date,
                    max_encounter_dist_km=config['max_encounter_dist_km'],
                    min_encounter_time_minutes=config['min_encounter_time_minutes'],

                    # GoogleCloud options
                    project=config['project_id'],
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    region='{region}'.format(**config),

                    # Worker options
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),

                    # Setup options
                    requirements_file='./requirements.txt',
                    setup_file='./setup.py'
                )
            )

            for source_exists in source_sensors:
                dag >> source_exists >> create_raw_encounters

            if not config.get('backfill', False):
                ensure_creation_tables = BigQueryCreateEmptyTableOperator(
                    task_id='ensure_raw_encounters_creation_tables',
                    dataset_id='{pipeline_dataset}'.format(**config),
                    table_id='{raw_table}'.format(**config),
                    schema_fields=[
                        { "type": "STRING", "name": "encounter_id", "mode": "REQUIRED" },
                        { "type": "TIMESTAMP", "name": "start_time", "mode": "REQUIRED" },
                        { "type": "TIMESTAMP", "name": "end_time", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "mean_latitude", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "mean_longitude", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "median_distance_km", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "median_speed_knots", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "start_lat", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "start_lon", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "end_lat", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "end_lon", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_1_id", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_1_seg_ids", "mode": "REPEATED" },
                        { "type": "INTEGER", "name": "vessel_1_point_count", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_2_id", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_2_seg_ids", "mode": "REPEATED" },
                        { "type": "INTEGER", "name": "vessel_2_point_count", "mode": "REQUIRED" }
                    ],
                    start_date_str=start_date,
                    end_date_str=end_date
                )

                segment_info_updated = BigQueryCheckOperator(
                    task_id='segment_info_updated',
                    sql='select count(*) from `{source_dataset}.{segment_info}` where date(last_timestamp) = "{ds}"'.format(**config),
                    use_legacy_sql=False,
                    retries=144,
                    retry_delay=timedelta(minutes=30),
                    max_retry_delay=timedelta(minutes=30),
                    on_failure_callback=config_tools.failure_callback_gfw
                )

                spatial_measures_existence = BigQueryCheckOperator(
                    task_id='spatial_measures_existence',
                    sql='select count(*) from `{spatial_measures_source}`'.format(**config),
                    use_legacy_sql=False,
                    retries=144,
                    retry_delay=timedelta(minutes=30),
                    max_retry_delay=timedelta(minutes=30),
                    on_failure_callback=config_tools.failure_callback_gfw
                )

                merge_encounters = DataFlowDirectRunnerOperator(
                    task_id='merge-encounters',
                    pool='dataflow',
                    py_file=python_target,
                    options=dict(
                        # Airflow
                        startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                                 'pipe_encounters/merge-encounters.log'),
                        command='{docker_run} {docker_image} merge_encounters'.format(**config),

                        # Required
                        start_date=self.default_args['start_date'].strftime("%Y-%m-%d"),
                        end_date=end_date,
                        raw_table='{project_id}.{pipeline_dataset}.{raw_table}'.format(**config),
                        sink_table='{project_id}:{pipeline_dataset}.{encounters_table}'.format(**config),
                        vessel_id_table='{source_dataset}.{segment_info}'.format(**config),
                        spatial_measures_table='{spatial_measures_source}'.format(**config),

                        # GoogleCloud options
                        project=config['project_id'],
                        runner='{dataflow_runner}'.format(**config),
                        temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                        staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                        region='{region}'.format(**config),

                        # Worker options
                        max_num_workers='{dataflow_max_num_workers}'.format(**config),
                        disk_size_gb='{dataflow_disk_size_gb}'.format(**config),

                        # Setup options
                        requirements_file='./requirements.txt',
                        setup_file='./setup.py'
                    )
                )

                create_raw_encounters >> ensure_creation_tables >> segment_info_updated >> merge_encounters
                create_raw_encounters >> ensure_creation_tables >> spatial_measures_existence >> merge_encounters

            return dag


for mode in ['daily', 'monthly', 'yearly']:
    dag_id='encounters_{}'.format(mode)
    globals()[dag_id] = PipeEncountersDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
