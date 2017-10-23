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
        msg = encounters._asdict()
        msg['vessel_1_id'] = id1
        msg['vessel_2_id'] = id2
        for key, value in self.mapping.items():
            msg[value] = msg.pop(key)
        return dict(msg)

    def expand(self, xs):
        return (
            xs
            | Map(self.create_messages)
        )
