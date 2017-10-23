from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
import apache_beam as beam
from collections import namedtuple
import datetime
import pytz
from statistics import median

from .create_encounter_records import Encounter

inf = float('inf')
epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

T = beam.typehints.TypeVariable('T')


class MergeEncounters(PTransform):

    def __init__(self, min_hours_between_encounters):
        self.min_hours_between_encounters = min_hours_between_encounters

    def key_by_ordered_mmsi(self, item):
        id_1, id_2, encounter = item
        if id_1 < id_2:
            return (id_1, id_2), item
        else:
            return (id_2, id_1), item

    def encounter_from_records(self, id_1, id_2, records):
        # n_points are the points that correspond to the means in the records
        n_points = max(sum(env.vessel_1_points for (env, p1, p2) in records), 1)
        return id_1, id_2, Encounter(
            start_time = min(env.start_time for (env, p1, p2) in records),
            end_time = max(env.end_time for (env, p1, p2) in records),
            mean_lat = sum(env.vessel_1_points * env.mean_lat 
                            for (env, p1, p2) in records) / n_points,
            mean_lon = sum(env.vessel_1_points * env.mean_lon 
                            for (env, p1, p2) in records) / n_points,
            # NOTE: this is the median of medians, not the true median
            # TODO: discuss with Nate
            median_speed_knots = median(env.median_distance_km for (env, p1, p2) in records),
            median_distance_km = median(env.median_distance_km for (env, p1, p2) in records),
            # These points correspond to key_id_?, not id_?
            vessel_1_points = sum(p1 for (env, p1, p2) in records),
            vessel_2_points = sum(p2 for (env, p1, p2) in records),
        )

    def merge_encounters(self, item):
        (key_id_1, key_id_2), id_encounters = item
        id_encounters = list(id_encounters)
        print("XXX", id_encounters[0])
        id_encounters.sort(key=lambda x: x[2].start_time)
        merged = []
        end = epoch
        records = None
        for (id1, id2, enc) in id_encounters:
            v1_pts = enc.vessel_1_points
            v2_pts = enc.vessel_2_points
            if id1 == key_id_2:
                v1_pts, v2_pts = v2_pts, v1_pts 
            rcd = (enc, v1_pts, v2_pts)
            # Records are ordered by start time. 
            #
            if enc.start_time - end > datetime.timedelta(self.min_hours_between_encounters):
                if records:
                    merged.append(self.encounter_from_records(key_id_1, key_id_2, records))
                records = [rcd]
            else:
                records.append(rcd)
            end = enc.end_time
        if records:
            merged.append(self.encounter_from_records(key_id_1, key_id_2, records))      
        return merged


    def expand(self, xs):
        return (
            xs
            | Map(self.key_by_ordered_mmsi).with_output_types(
                beam.typehints.Tuple[beam.typehints.Tuple[int, int], T])
            | "Group by Orderred MMSI" >> GroupByKey()
            | FlatMap(self.merge_encounters)
        )
