from apache_beam import PTransform
from apache_beam import Map
from collections import namedtuple

class Encounter(namedtuple("Encounter",
    ["start_time", "end_time", "mean_lat", "mean_lon", "median_distance_km",
    "median_speed_knots", "vessel_1_points", "vessel_2_points"])):

    __slots__ = ()


class CreateEncounterRecords(PTransform):

    # TODO: share mapping with create_messages (rename encounter messages and put both these there?)
    mapping = {
        'mean_lat': 'mean_latitude',
        'mean_lon': 'mean_longitude',
        'vessel_1_points': 'vessel_1_point_count',
        'vessel_2_points': 'vessel_2_point_count',
        }

    def create_encounter(self, msg):
        id1 = msg.pop('vessel_1_id')
        id2 = msg.pop('vessel_2_id')
        for k1, k2 in self.mapping.items():
            msg[k1] = msg.pop(k2)
        return (id1, id2, Encounter(**msg))

    def expand(self, xs):
        return (
            xs
            | Map(self.create_encounter)
        )
