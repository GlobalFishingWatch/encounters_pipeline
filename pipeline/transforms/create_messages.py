from apache_beam import PTransform
from apache_beam import Map

class CreateMessages(PTransform):

    mapping = {
        'mean_lat': 'mean_latitude',
        'mean_lon': 'mean_longitude',
        'vessel_1_points': 'vessel_1_point_count',
        'vessel_2_points': 'vessel_2_point_count',
        }

    def create_messages(self, value):
        id1, id2, encounters = value
        assert id1 == encounters.vessel_1_id
        assert id2 == encounters.vessel_2_id
        return encounters._asdict()

    def expand(self, xs):
        return (
            xs
            | Map(self.create_messages)
        )
