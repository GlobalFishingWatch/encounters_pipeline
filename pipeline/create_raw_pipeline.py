import datetime
import logging
import pytz

from apache_beam import io
from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
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
PRECURSOR_DAYS = 1


def create_queries(options):
    create_options = options.view_as(CreateOptions)
    template = """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      UNIX_MILLIS(a.timestamp) / 1000.0  AS timestamp,
      CONCAT("{id_prefix}", {vessel_id}) AS id
    FROM
      (SELECT *, _TABLE_SUFFIX FROM `{position_table}*` 
        WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' AND
              lat   IS NOT NULL AND
              lon   IS NOT NULL AND
              speed IS NOT NULL) a
    INNER JOIN
      (SELECT *, _TABLE_SUFFIX FROM `{segment_table}*` 
        WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' AND
        noise = FALSE) b
    USING(_TABLE_SUFFIX, seg_id)

    """
    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d') 
    start_of_full_window = start_date - datetime.timedelta(days=PRECURSOR_DAYS)
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d') 

    vessel_id_txt = 'vessel_id' if (create_options.vessel_id_column is None) else create_options.vessel_id_column

    for dataset in create_options.source_datasets:
        if '::' in dataset:
            id_prefix, dataset = dataset.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        dataset = dataset.replace(':', '.')
        start_window = start_of_full_window
        position_table = dataset + '.position_messages_'
        segment_table = dataset + '.segments_'
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(id_prefix=id_prefix, position_table=position_table, segment_table=segment_table,
                            start=start_window, end=end_window, vessel_id=vessel_id_txt, min_message_count=2)
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

    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x, project=cloud_options.project,
                                                                                  use_standard_sql=True)))
                    for (i, x) in enumerate(create_queries(options))]


    adjacencies = (sources
        | Flatten()
        | Record.FromDict()
        | Resample(increment_s = 60 * RESAMPLE_INCREMENT_MINUTES, 
                   max_gap_s = 60 * 60 * MAX_GAP_HOURS) 
        | ComputeAdjacency(max_adjacency_distance_km=create_options.max_encounter_dist_km) 
        )

    (adjacencies
        | ComputeEncounters(max_km_for_encounter=create_options.max_encounter_dist_km, 
                            min_minutes_for_encounter=create_options.min_encounter_time_minutes) 
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

    success_states = set([PipelineState.DONE])

    if create_options.wait or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1