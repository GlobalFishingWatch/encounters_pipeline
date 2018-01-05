from collections import namedtuple
from .namedtuples import NamedtupleCoder
from .resampled_record import ResampledRecord as _ResampledRecord


AnnotatedRecord = namedtuple("AnnotatedRecord", 
    _ResampledRecord._fields + ("neighbor_count", "closest_neighbor", "closest_distance"))


class AnnotatedRecordCoder(NamedtupleCoder):
    target = AnnotatedRecord
    time_fields = ['timestamp']


AnnotatedRecordCoder.register()

