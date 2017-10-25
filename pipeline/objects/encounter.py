from collections import namedtuple
from .namedtuples import NamedtupleCoder

Encounter = namedtuple("Encounter", 
    ["vessel_1_id", "vessel_2_id", "start_time", "end_time", 
     "mean_latitude", "mean_longitude", 
     "median_distance_km", "median_speed_knots", 
     "vessel_1_point_count", "vessel_2_point_count"])


class EncounterCoder(NamedtupleCoder):
    target = Encounter
    time_fields = ['start_time', 'end_time']


EncountersFromTuples, EncountersFromDicts, EncountersToDicts = EncounterCoder.register()
