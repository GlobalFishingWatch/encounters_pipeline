from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
import datetime
import logging
import math
from collections import defaultdict
from collections import namedtuple
import itertools as it
from statistics import median
from statistics import mean
from .create_encounter_records import Encounter as SingleEncounter
from .compute_adjacency import compute_distance as compute_distance_km

MPS_TO_KNOTS = 1.94384

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = it.tee(iterable)
    next(b, None)
    return it.izip(a, b)


def implied_speed_mps(rcd1, rcd2):
    distance = 1000 * compute_distance_km(rcd1, rcd2) 
    duration = (rcd2.timestamp - rcd1.timestamp).total_seconds() # s
    return distance / duration



class ComputeEncounters(PTransform):


    def __init__(self, max_km_for_encounter, min_minutes_for_encounter):
        self.min_minutes_for_encounter = min_minutes_for_encounter
        self.max_km_for_encounter = max_km_for_encounter


    def compute_encounters(self, item):

        vessel_id, records = item

        def try_adding_encounter_vessel():
            if (current_encounter_id is not None) and (len(current_run) >= 2):
                start_time = current_run[0].record.timestamp
                end_time = current_run[-1].record.timestamp
                encounter_duration = end_time - start_time

                if encounter_duration >= datetime.timedelta(minutes=self.min_minutes_for_encounter):
                    implied_speeds = [implied_speed_mps(ar1.record, ar2.record) for 
                                                (ar1, ar2) in pairwise(current_run)]

                    median_distance_km = median(x.closest_distance for x in current_run)
                    mean_lat = mean(x.record.lat for x in current_run)
                    mean_lon = mean(x.record.lon for x in current_run)
                    median_speed_knots = median(implied_speeds) * MPS_TO_KNOTS

                    vessel_1_points = int(round(sum(x.record.point_density for x in current_run)))
                    vessel_2_points = int(round(sum(x.closest_neighbor.point_density for x in current_run)))

                    key = (vessel_id, current_encounter_id)

                    encounters[key].append(
                        SingleEncounter(start_time,
                                        end_time,
                                        mean_lat,
                                        mean_lon,
                                        median_distance_km,
                                        median_speed_knots,
                                        vessel_1_points,
                                        vessel_2_points))
            current_run[:] = []


        encounters = defaultdict(list)

        current_encounter_id = id
        current_run = []

        for l in records:
            is_possible_encounter_pt = (l.closest_distance <= self.max_km_for_encounter)

            if is_possible_encounter_pt:
                closest_id = l.closest_neighbor.id
                if current_encounter_id not in (None, closest_id):
                    try_adding_encounter_vessel()
                    current_encounter_id = closest_id
                else:
                    current_run.append(l)
                current_encounter_id = closest_id
            else:
                try_adding_encounter_vessel()
                current_encounter_id = None

        try_adding_encounter_vessel()

        return [(key[0], key[1], x) 
                    for (key, value) in encounters.items() 
                        for x in value]


    def expand(self, xs):
        return (
            xs
            | FlatMap(self.compute_encounters)
        )
 