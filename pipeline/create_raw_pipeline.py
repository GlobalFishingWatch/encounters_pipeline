import datetime
import logging
import pytz

from apache_beam import io
from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.io import WriteToBigQueryDatePartitioned
from pipeline.objects.encounter import Encounter
from pipeline.objects.record import Record
from pipeline.options.create_options import CreateOptions
from pipeline.schemas.nbr_count_output import build as nbr_count_build_schema
from pipeline.schemas.output import build as output_build_schema
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.sort_by_time import SortByTime
from pipeline.transforms.resample import Resample
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.transforms.create_timestamped_adjacencies import CreateTimestampedAdjacencies
from pipeline.transforms.writers import WriteToBq



RESAMPLE_INCREMENT_MINUTES = 10.0
MAX_GAP_HOURS = 1.0
MAX_ENCOUNTER_DISTANCE_KM = 0.5
MIN_ENCOUNTER_TIME_MINUTES = 120.0
PRECURSOR_DAYS = 1


def create_queries(options):
    create_options = options.view_as(CreateOptions)
    template = """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
      CONCAT("{id_prefix}", vessel_id) AS id
    FROM
      TABLE_DATE_RANGE([{table}], 
                            TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}'))
    WHERE
      lat   IS NOT NULL AND
      lon   IS NOT NULL AND
      speed IS NOT NULL
    """
    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d') 
    start_of_full_window = start_date - datetime.timedelta(days=PRECURSOR_DAYS)
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d') 
    for table in create_options.source_tables:
        if '::' in table:
            id_prefix, table = table.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        start_window = start_of_full_window
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(id_prefix=id_prefix, table=table, start=start_window, end=end_window)
            print(query)
            yield query
            start_window = end_window + datetime.timedelta(days=1)

def run(options):

    p = Pipeline(options=options)

    create_options = options.view_as(CreateOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    writer = WriteToBigQueryDatePartitioned(
                temp_gcs_location=cloud_options.temp_location,
                table=create_options.raw_table,
                write_disposition="WRITE_TRUNCATE",
                schema=output_build_schema(),
                project=cloud_options.project
                )

    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x, project=cloud_options.project)))
                    for (i, x) in enumerate(create_queries(options))]


    adjacencies = (sources
        | Flatten()
        | Record.FromDict()
        | Resample(increment_s = 60 * RESAMPLE_INCREMENT_MINUTES, 
                   max_gap_s = 60 * 60 * MAX_GAP_HOURS) 
        | ComputeAdjacency(max_adjacency_distance_km=MAX_ENCOUNTER_DISTANCE_KM) 
        )

    (adjacencies
        | ComputeEncounters(max_km_for_encounter=MAX_ENCOUNTER_DISTANCE_KM, 
                            min_minutes_for_encounter=MIN_ENCOUNTER_TIME_MINUTES) 
        | Filter(lambda x: start_date.date() <= x.end_time.date() <= end_date.date())
        | Encounter.ToDict()
        | Map(lambda x: TimestampedValue(x, x['end_time'])) 
        | writer
    )

    if create_options.neighbor_table:
        (adjacencies
            | CreateTimestampedAdjacencies(start_date, end_date)
            | "WriteNeighbors" >> WriteToBigQueryDatePartitioned(
                temp_gcs_location=cloud_options.temp_location,
                table=create_options.neighbor_table,
                write_disposition="WRITE_TRUNCATE",
                schema=nbr_count_build_schema(),
                project=cloud_options.project
                )
            )


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1