from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam import io
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState

from pipeline.objects.encounter import Encounter, RawEncounter
from pipeline.options.merge_options import MergeOptions
from pipeline.transforms.filter_inland import FilterInland
from pipeline.transforms.filter_ports import FilterPorts
from pipeline.transforms.add_id import AddEncounterId
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.writers import WriteToBq
from pipeline.schemas.output import build as build_schema

import datetime
import logging
import pytz
import six


def combine_ids(obj):
    for v in [1, 2]:
        obj[f'vessel_{v}_seg_id'] = (six.ensure_binary(obj.pop(f'vessel_{v}_id')), 
                                 six.ensure_binary(obj[f'vessel_{v}_seg_id']))
    return obj

def create_queries(args, start_date, end_date):
    template = """
    WITH

    raw_encounters as (
        SELECT * except (start_time, end_time, encounter_id), -- TODO: do raw_encounters need an id?
                CAST(UNIX_MICROS(start_time) AS FLOAT64) / 1000000 AS start_time,
                CAST(UNIX_MICROS(end_time) AS FLOAT64) / 1000000 AS end_time
        FROM `{raw_table}*` events
        WHERE _table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' 
          {condition}
    ),

    vessel_ids as (
        {vessel_id_query}
    )

    SELECT raw_encounters.*,
            vid1.vessel_id as vessel_1_id,
            vid2.vessel_id as vessel_2_id
    FROM raw_encounters
    JOIN vessel_ids as vid1
    ON vessel_1_seg_id = vid1.seg_id
    JOIN vessel_ids as vid2
    ON vessel_2_seg_id = vid2.seg_id

    """
    if args.bad_segs_table is None:
        condition = ''
    else:
        # TODO: need multiple bad segs tables to support multiple inputs. Need optional 'prefix::' for them
        condition = f'  AND seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})'

    subqueries = []
    for table in args.vessel_id_tables:
        if '::' in table:
            id_prefix, table = table.split('::', 1)
            id_prefix += ':'
        else:
            id_prefix = ''
        table = table.replace(':', '.')
        subqueries.append(f'SELECT CONCAT("{id_prefix}", seg_id) AS seg_id, vessel_id FROM {table}')
    vessel_id_query = '\nUNION ALL\n'.join(subqueries)

    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(raw_table=args.raw_table, 
                                condition=condition,
                                vessel_id_query=vessel_id_query,
                                start=start_window, end=end_window)
        yield query
        start_window = end_window + datetime.timedelta(days=1)


def run(options):

    p = Pipeline(options=options)

    merge_options = options.view_as(MergeOptions)

    # writer_filtered = WriteToBq(
    #     table=merge_options.sink_table,
    #     write_disposition="WRITE_TRUNCATE",
    # )

    writer = io.WriteToBigQuery(
        merge_options.sink_table,
        schema=build_schema(),
        write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}})


    start_date = datetime.datetime.strptime(merge_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(merge_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    queries = create_queries(merge_options, start_date, end_date)

    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x,
                            use_standard_sql=True)))
                    for (i, x) in enumerate(queries)]

    raw_encounters = (sources
        | Flatten()
        | "CombineSegVesselIds" >> Map(combine_ids)
        | RawEncounter.FromDict()
    )


    merged  = (raw_encounters
        | FilterPorts()
        | FilterInland()
        | MergeEncounters(min_hours_between_encounters=merge_options.min_hours_between_encounters)
    )

    if merge_options.min_encounter_time_minutes is not None:
        merged = (merged
            | Filter(lambda x: (x.end_time - x.start_time).total_seconds() / 60.0 >= 
                                merge_options.min_encounter_time_minutes)
        )

    merged = (merged
        | "FilteredToDicts" >> Encounter.ToDict()
        | AddEncounterId()
        | writer
    )

    result = p.run()

    success_states = set([PipelineState.DONE])

    if merge_options.wait or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1

