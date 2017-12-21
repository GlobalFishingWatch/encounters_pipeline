from apache_beam import io
from apache_beam import Filter
from apache_beam import Map
from apache_beam import PTransform
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.coders.jsoncoder import JSONDict
from pipeline.objects.namedtuples import _datetime_to_s


class CreateTimestampedAdjacencies(PTransform):

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date


    def extract_nbr_dict(item):
        return JSONDict(vessel_id=item.id, 
                        timestamp=_datetime_to_s(item.timestamp), 
                        neighbor_count=item.neighbor_count)


    def expand(self, xs):
        return (xs
            | Filter(lambda x: self.fstart_date <= x.timestamp <= self.end_date)
            | Map(self.extract_nbr_dict)
            | Map(lambda x: TimestampedValue(x, x['timestamp']))
        )