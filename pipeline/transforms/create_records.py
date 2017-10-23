from apache_beam import PTransform
from apache_beam import Map
from collections import namedtuple

class Record(namedtuple("Records",
    ["id", "timestamp", "lat", "lon", "speed"])):

    __slots__ = ()


class CreateRecords(PTransform):

    def create_record(self, msg):
        return Record(**msg)

    def expand(self, xs):
        return (
            xs
            | Map(self.create_record)
        )


# from ..objects.namedtuples import Record
# from ..objects.namedtuples import RecordFromDict as CreateRecords