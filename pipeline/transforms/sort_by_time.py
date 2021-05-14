from __future__ import division
from collections import defaultdict
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import typehints

from ..objects.record import Record


def median(iterable):
    seq = sorted(iterable)
    quotient, remainder = divmod(len(seq), 2)
    if remainder:
        return seq[quotient]
    return sum(seq[quotient - 1:quotient + 1]) / 2


class SortByTime(PTransform):

    def sort_and_uniquify_by_time(self, item):
        key, records = item
        time_map = defaultdict(list)

        for rcd in records:
            time_map[rcd.timestamp].append(rcd)

        sorted_records = []
        for t in sorted(time_map):
            records_at_t = time_map[t]
            id_ = records_at_t[0].id
            assert [x.id == id_ for x in records_at_t]
            sorted_records.append(Record(
                id = id_,
                timestamp = t,
                lat = median(x.lat for x in records_at_t),
                lon = median(x.lon for x in records_at_t),
                speed = median(x.speed for x in records_at_t),
                ))

        return key, sorted_records

    def expand(self, xs):
        return (
            xs
            | Map(self.sort_and_uniquify_by_time)
        )