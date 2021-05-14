from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam import io
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.io import WriteToBigQueryDatePartitioned
from pipeline.objects.encounter import RawEncounter
from pipeline.objects.record import Record
from pipeline.options.create_options import CreateOptions
from pipeline.schemas.output import build_raw_encounter
from pipeline.schemas.utils import schema_to_obj
from pipeline.transforms.add_id import AddRawEncounterId
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.transforms.create_timestamped_adjacencies import CreateTimestampedAdjacencies
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.resample import Resample
from pipeline.transforms.sort_by_time import SortByTime
from pipeline.transforms.writers import WriteToBq

import datetime
import logging
import pytz
import six



RESAMPLE_INCREMENT_MINUTES = 10.0
MAX_GAP_HOURS = 1.0
PRECURSOR_DAYS = 1


def create_queries(args):
    template = """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      UNIX_MILLIS(timestamp) / 1000.0  AS timestamp,
      CONCAT("{id_prefix}", seg_id) AS id
    FROM
        `{position_table}*`
    WHERE
        _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
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
    start_of_full_window = start_date - datetime.timedelta(days=PRECURSOR_DAYS)
    end_date= datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    for table in args.source_tables:
        if '::' in table:
            id_prefix, table = table.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        table = table.replace(':', '.')
        start_window = start_of_full_window
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(id_prefix=id_prefix, position_table=table, 
                                    start=start_window, end=end_window,
                                    condition=condition
                                    )
            yield query
            start_window = end_window + datetime.timedelta(days=1)

# def ensure_bytes_id(obj):
#     return obj._replace(id=six.ensure_binary(obj.id))

# def ensure_text_seg_ids(mapping):
#     try:
#         mapping['vessel_1_seg_id'] = six.ensure_text(mapping['vessel_1_seg_id'])
#         mapping['vessel_2_seg_id'] = six.ensure_text(mapping['vessel_2_seg_id'])
#     except:
#         raise ValueError(f'could not turn to text {mapping["vessel_1_seg_id"]}, {mapping["vessel_2_seg_id"]}')
#     return mapping


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
                schema=build_raw_encounter(),
                project=cloud_options.project
                )

    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x, project=cloud_options.project,
                                                                                  use_standard_sql=True)))
                    for (i, x) in enumerate(create_queries(create_options))]


    adjacencies = (sources
        | Flatten()
        | Record.FromDict()
        # | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
        | Resample(increment_s = 60 * RESAMPLE_INCREMENT_MINUTES, 
                   max_gap_s = 60 * 60 * MAX_GAP_HOURS) 
        | ComputeAdjacency(max_adjacency_distance_km=create_options.max_encounter_dist_km) 
        | ComputeEncounters(max_km_for_encounter=create_options.max_encounter_dist_km, 
                            min_minutes_for_encounter=create_options.min_encounter_time_minutes) 
        | Filter(lambda x: start_date.date() <= x.end_time.date() <= end_date.date())
        | RawEncounter.ToDict()
        | AddRawEncounterId()
        # | Map(ensure_text_seg_ids)
        | Map(lambda x: TimestampedValue(x, x['end_time'])) 
        | Map(check_schema, schema=schema_to_obj(build_raw_encounter()))
        | writer
    )

    result = p.run()

    success_states = set([PipelineState.DONE])

    if create_options.wait or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1
