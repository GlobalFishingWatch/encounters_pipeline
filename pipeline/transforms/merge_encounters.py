from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
import apache_beam as beam
import datetime
import pytz
from statistics import median

from ..objects.encounter import Encounter

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
        # n_points are the points that correspond to the means in the records
        n_points = max(sum(env.vessel_1_point_count for (env, p1, p2) in records), 1)
        return Encounter(
            vessel_1_id = id_1,
            vessel_2_id = id_2,
            start_time = min(env.start_time for (env, p1, p2) in records),
            end_time = max(env.end_time for (env, p1, p2) in records),
            mean_latitude = sum(env.vessel_1_point_count * env.mean_latitude 
                            for (env, p1, p2) in records) / n_points,
            mean_longitude = sum(env.vessel_1_point_count * env.mean_longitude
                            for (env, p1, p2) in records) / n_points,
            # NOTE: this is the median of medians, not the true median
            # TODO: discuss with Nate
            median_speed_knots = median(env.median_speed_knots for (env, p1, p2) in records),
            median_distance_km = median(env.median_distance_km for (env, p1, p2) in records),
            # These points correspond to key_id_?, not id_?
            vessel_1_point_count = sum(p1 for (env, p1, p2) in records),
            vessel_2_point_count = sum(p2 for (env, p1, p2) in records),
        )

    def merge_encounters(self, item):
        (key_id_1, key_id_2), encounters = item
        encounters = list(encounters)
        encounters.sort(key=lambda x: x.start_time)
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
            # Records are ordered by start time. 
            #
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
                beam.typehints.Tuple[beam.typehints.Tuple[int, int], T])
            | "Group by Orderred MMSI" >> GroupByKey()
            | FlatMap(self.merge_encounters)
        )
