from collections import namedtuple
from .namedtuples import NamedtupleCoder
from .record import Record as _Record


ResampledRecord = namedtuple("ResampledRecord", _Record._fields + ('point_density',))


class ResampledRecordCoder(NamedtupleCoder):
    target = ResampledRecord
    time_fields = ['timestamp']


ResampledRecordsFromTuples, ResampledRecordsFromDicts, ResampledResampledRecordsToDicts = ResampledRecordCoder.register()

  