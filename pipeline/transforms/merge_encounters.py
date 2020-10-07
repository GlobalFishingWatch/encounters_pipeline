from ..objects.encounter import Encounter
from apache_beam import FlatMap
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import PTransform
from statistics import median

import apache_beam as beam
import datetime
import math
import pytz
import six

inf = float('inf')
epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

T = beam.typehints.TypeVariable('T')


class MergeEncounters(PTransform):

    def __init__(self, min_hours_between_encounters):
        self.min_hours_between_encounters = min_hours_between_encounters

    def key_by_ordered_mmsi(self, encounter):
        id_1 = encounter.vessel_1_id
        id_2 = encounter.vessel_2_id
        if id_1 < id_2:
            return (id_1, id_2), encounter
        else:
            return (id_2, id_1), encounter

    def encounter_from_records(self, id_1, id_2, records):
        total_seconds = max(sum((env.end_time - env.start_time).total_seconds() 
                                    for (env, p1, p2) in records), 1)
        start_enc, _, _ = records[0]
        end_enc = min(records, key=lambda x : x[0].end_time)[0]

        cos_lon = sum((env.end_time - env.start_time).total_seconds() * 
                        math.cos(math.radians(env.mean_longitude))
                            for (env, p1, p2) in records) / total_seconds
        sin_lon = sum((env.end_time - env.start_time).total_seconds() * 
                        math.sin(math.radians(env.mean_longitude))
                            for (env, p1, p2) in records) / total_seconds
        return Encounter(
            vessel_1_id = id_1,
            vessel_2_id = id_2,
            start_time = min(env.start_time for (env, p1, p2) in records),
            end_time = max(env.end_time for (env, p1, p2) in records),
            mean_latitude = sum((env.end_time - env.start_time).total_seconds() * env.mean_latitude 
                            for (env, p1, p2) in records) / total_seconds,
            mean_longitude = math.degrees(math.atan2(sin_lon, cos_lon)),
            # NOTE: this is the median of medians, not the true median
            # TODO: discuss with Nate
            median_speed_knots = median(env.median_speed_knots for (env, p1, p2) in records),
            median_distance_km = median(env.median_distance_km for (env, p1, p2) in records),
            # These points correspond to key_id_?, not id_?
            vessel_1_point_count = sum(p1 for (env, p1, p2) in records),
            vessel_2_point_count = sum(p2 for (env, p1, p2) in records),
            start_lat = start_enc.start_lat,
            start_lon = start_enc.start_lon,
            end_lat = end_enc.end_lat,
            end_lon = end_enc.end_lon
        )

    def merge_encounters(self, item):
        (key_id_1, key_id_2), encounters = item
        encounters = list(encounters)
        encounters.sort(key=lambda x: (x.start_time, x.end_time, x.vessel_1_id, x.vessel_2_id))
        end = epoch
        records = None
        for enc in encounters:
            id1 = enc.vessel_1_id
            id2 = enc.vessel_2_id
            v1_pts = enc.vessel_1_point_count
            v2_pts = enc.vessel_2_point_count
            if id1 == key_id_2:
                v1_pts, v2_pts = v2_pts, v1_pts 
            rcd = (enc, v1_pts, v2_pts)
            # Records are ordered by start time, end_time and vessel_ids.
            if enc.start_time - end >= datetime.timedelta(hours=self.min_hours_between_encounters):
                if records:
                    yield self.encounter_from_records(key_id_1, key_id_2, records)
                records = [rcd]
            else:
                records.append(rcd)
            end = enc.end_time
        if records:
            yield self.encounter_from_records(key_id_1, key_id_2, records)


    def expand(self, xs):
        return (
            xs
            | Map(self.key_by_ordered_mmsi).with_output_types(
                beam.typehints.Tuple[beam.typehints.Tuple[six.binary_type, six.binary_type], T])
            | "Group by Ordered MMSI" >> GroupByKey()
            | FlatMap(self.merge_encounters)
        )
