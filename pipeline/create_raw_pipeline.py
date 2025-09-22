from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue

from google.cloud import bigquery

from pipeline.objects.encounter import RawEncounter
from pipeline.objects.record import Record
from pipeline.options.create_options import CreateOptions
from pipeline.schemas.adjacency_encounter import adjacency_encounter
from pipeline.schemas.utils import schema_to_obj
from pipeline.transforms.add_id import AddRawEncounterId
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.transforms.resample import Resample
from pipeline.transforms.readers import ReadSources
from pipeline.transforms.sink import AdjacencyEncountersSink

from pipeline.utils.bqtools import BigQueryHelper, DatePartitionedTable
from pipeline.utils.ver import get_pipe_ver


import datetime
import logging
import pytz



PRECURSOR_DAYS = 1


def create_queries(args):
    template = """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      course     AS course,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("{id_prefix}", seg_id) AS id
    FROM
        `{position_table}`
    WHERE
        date(timestamp) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
        {condition}
    """
    if args.ssvid_filter is None:
        condition = ''
    else:
        filter_core = args.ssvid_filter
        if filter_core.startswith('@'):
            with open(args.ssvid_filter[1:]) as f:
                filter_core = f.read()
        condition = f'AND ssvid in ({filter_core})'

    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date= datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    for table in args.source_tables:
        if '::' in table:
            id_prefix, table = table.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        start_window = start_date
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(id_prefix=id_prefix, position_table=table,
                                    start=start_window, end=end_window,
                                    condition=condition
                                    )
            logging.info(f"Generated query for {table}: {query}")
            yield query
            start_window = end_window + datetime.timedelta(days=1)


def check_schema(x, schema):
    assert set(x.keys()) == set([x['name'] for x in schema]), x.keys()
    for field in schema:
        assert field['mode'] == 'REQUIRED'
        # TODO: support 'NULLABLE' and 'REPEATED'
        ftype = field['type']
        val = x[field['name']]
        allowed_types_map = {
            'STRING' : (str,),
            'INTEGER' : (int,),
            'FLOAT' : (int, float),
            'TIMESTAMP' : (int, float)
        }
        if ftype not in allowed_types_map:
             raise ValueError(f'unknown schema type {field}') 
        allowed_types = allowed_types_map[ftype]
        assert isinstance(val, allowed_types), (field, val)
    return x


def prepare_output_tables(pipe_options, cloud_options, start_date, end_date):
    output_table = DatePartitionedTable(
        table_id=pipe_options.adjacency_table,
        description=f"""
Created by the encounters_pipeline: {get_pipe_ver()}.
* Creates adjacency positions on a {pipe_options.resample_increment_minutes} minute time grid based on which encounters are generated.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Sources: {pipe_options.source_tables}
* Maximum distance for vessels to be elegible (km): {pipe_options.max_encounter_dist_km}
        """,  # noqa: E501
        schema=adjacency_encounter["fields"],
        partitioning_field="timestamp",
    )

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=cloud_options.project,
        ),
        labels=dict([entry.split("=") for entry in cloud_options.labels]),
    )

    bq_helper.ensure_table_exists(output_table)
    bq_helper.update_table(output_table)


def adjacency_to_msg(adjacency_record):
    adjacency = dict()
    adjacency["timestamp"] = adjacency_record.timestamp
    adjacency["seg_id"] = adjacency_record.id
    adjacency["lat"] = adjacency_record.lat
    adjacency["lon"] = adjacency_record.lon
    # adjacency_segments is an array of two attributes: adjacency_record.closest_neighbors and adjacency_record.closest_distances
    # both are arrays that need to be zipped
    adjacency["adjacent_segments"] = [{
        'seg_id': neighbor.id,
        'lat': neighbor.lat,
        'lon': neighbor.lon,
        'distance': distance
    } for neighbor, distance in zip(adjacency_record.closest_neighbors, adjacency_record.closest_distances)]
    return adjacency

def run(options):

    p = Pipeline(options=options)

    create_options = options.view_as(CreateOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    cloud_options.labels = [
        "pipeline=encounters",
        "job=create_raw_pipeline",
        "source=bigquery",
        "type=raw",
        "creator=christianhomberg"
    ]

    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    sources = [
        (p | f"Read_{i}" >> ReadSources(
            query=query,
            options=cloud_options
        )) for (i, query) in enumerate(create_queries(create_options))
    ]


    (sources
        | Flatten()
        | Record.FromDict()
        | Resample(increment_s = 60 * create_options.resample_increment_minutes,
                   max_gap_s = 60 * 60 * create_options.max_gap_hours)
        | ComputeAdjacency(max_adjacency_distance_km=create_options.max_encounter_dist_km)
        | Map(adjacency_to_msg)
        | AdjacencyEncountersSink(create_options.adjacency_table)
    )
    
    prepare_output_tables(create_options, cloud_options, start_date, end_date)
    result = p.run()

    success_states = set([PipelineState.DONE])

    if create_options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
