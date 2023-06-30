from apache_beam import CoGroupByKey
from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import FlatMap
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam import io
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState

from pipeline.objects.encounter import Encounter, RawEncounter
from pipeline.options.merge_options import MergeOptions
from pipeline.transforms.add_id import AddEncounterId
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.readers import ReadSources
from pipeline.transforms.writers import WriteEncountersToBQ
from pipeline.schemas.output import build as build_schema

import datetime
import logging
import numpy as np
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
        SELECT * except (start_time, end_time, encounter_id), 
                CAST(UNIX_MICROS(start_time) AS FLOAT64) / 1000000 AS start_time,
                CAST(UNIX_MICROS(end_time) AS FLOAT64) / 1000000 AS end_time,
                format("lon:%+07.2f_lat:%+07.2f", mean_longitude, mean_latitude) as gridcode,
        FROM `{raw_table}*` events
        WHERE _table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' 
          {condition}
    ),

    vessel_ids as (
        {vessel_id_query}
    )

    SELECT raw_encounters.* except(gridcode),
            vid1.vessel_id as vessel_1_id,
            vid2.vessel_id as vessel_2_id,
            distance_from_shore_m, distance_from_port_m
    FROM raw_encounters
    JOIN vessel_ids as vid1
    ON vessel_1_seg_id = vid1.seg_id
    JOIN vessel_ids as vid2
    ON vessel_2_seg_id = vid2.seg_id
    JOIN  `{spatial_measures_table}`
    USING (gridcode)
    """
    if args.bad_segs_table is None:
        condition = ''
    else:
        # TODO: need multiple bad segs tables to support multiple inputs. Need optional 'prefix::' for them
        condition = f'''  AND vessel_1_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})
                          AND vessel_2_seg_id NOT IN (SELECT seg_id FROM {args.bad_segs_table})
        '''

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
                                start=start_window, end=end_window,
                                spatial_measures_table=args.spatial_measures_table)
        yield query
        start_window = end_window + datetime.timedelta(days=1)



def filter_by_distance(obj, min_distance_from_port_km):
    distance_from_shore_m = obj.pop('distance_from_shore_m')
    distance_from_port_m = obj.pop('distance_from_port_m')
    if distance_from_port_m < min_distance_from_port_km * 1000:
        return []
    elif distance_from_shore_m <= 0:
        return []
    return [obj]

def tag_with_gridcode(x):
    lat = np.clip(x.mean_latitude, -89.99, 89.99)
    gc = f"lon:{round(x.mean_longitude, 2):+07.2f}_lat:{round(lat, 2):+07.2f}"
    gc = gc.replace('-000.00', '+000.00').replace('+180.00', '-180.00')
    return (gc, x)

def run(options):

    p = Pipeline(options=options)

    merge_options = options.view_as(MergeOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    dists_query = f"""
    select gridcode, distance_from_shore_m, distance_from_port_m
    from `{merge_options.spatial_measures_table}`
    """

    start_date = datetime.datetime.strptime(merge_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(merge_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    queries = create_queries(merge_options, start_date, end_date)

    sources = [
        (p | f"Read_{i}" >> ReadSources(
            query=query,
            options=cloud_options
        )) for (i, query) in enumerate(queries)
    ]

    merged = (sources
        | Flatten()
        | "CombineSegVesselIds" >> Map(combine_ids)
        | FlatMap(filter_by_distance, min_distance_from_port_km=10)
        | RawEncounter.FromDict()
        | MergeEncounters(min_hours_between_encounters=merge_options.min_hours_between_encounters)
    )

    if merge_options.min_encounter_time_minutes is not None:
        merged = (merged
            | Filter(lambda x: (x.end_time - x.start_time).total_seconds() / 60.0 >= 
                                merge_options.min_encounter_time_minutes)
        )

    writer = WriteEncountersToBQ(merge_options, cloud_options)

    merged = (merged
        | "FilteredToDicts" >> Encounter.ToDict()
        | AddEncounterId()
        | writer
    )

    result = p.run()

    success_states = set([PipelineState.DONE])

    if merge_options.wait_for_job or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
        if result.state in success_states:
            writer.update_table_description()
            writer.update_labels()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)
        success_states.add(PipelineState.PENDING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1

