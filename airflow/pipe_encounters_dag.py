from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable

from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator

import posixpath as pp


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
                    position_messages_table='{position_messages_table}'.format(**config),
                    segments_table='{segments_table}'.format(**config),
                    raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                    neighbor_table='{project_id}:{pipeline_dataset}.{neighbor_table}'.format(**config),
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
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
                        { "type": "TIMESTAMP", "name": "start_time", "mode": "REQUIRED" },
                        { "type": "TIMESTAMP", "name": "end_time", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "mean_latitude", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "mean_longitude", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "median_distance_km", "mode": "REQUIRED" },
                        { "type": "FLOAT", "name": "median_speed_knots", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_1_id", "mode": "REQUIRED" },
                        { "type": "INTEGER", "name": "vessel_1_point_count", "mode": "REQUIRED" },
                        { "type": "STRING", "name": "vessel_2_id", "mode": "REQUIRED" },
                        { "type": "INTEGER", "name": "vessel_2_point_count", "mode": "REQUIRED" }
                    ],
                    start_date_str=start_date,
                    end_date_str=end_date
                )

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
                        start_date=self.default_args['start_date'].strftime("%Y-%m-%d"),
                        end_date=end_date,
                        raw_table='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                        sink='{project_id}:{pipeline_dataset}.{encounters_table}'.format(**config),
                        temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                        staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                        max_num_workers='{dataflow_max_num_workers}'.format(**config),
                        disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
                        requirements_file='./requirements.txt',
                        setup_file='./setup.py'
                    )
                )

                create_raw_encounters >> ensure_creation_tables >> merge_encounters

            return dag


for mode in ['daily', 'monthly', 'yearly']:
    dag_id='encounters_{}'.format(mode)
    globals()[dag_id] = PipeEncountersDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
