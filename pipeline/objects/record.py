from collections import namedtuple
from .namedtuples import NamedtupleCoder

Record = namedtuple("Record", ["id", "timestamp", "lat", "lon", "speed"])


class RecordCoder(NamedtupleCoder):
    target = Record
    time_fields = ['timestamp']


RecordsFromTuples, RecordsFromDicts, RecordsToDicts = RecordCoder.register()





