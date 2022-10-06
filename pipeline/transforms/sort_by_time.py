from __future__ import division

import math
from collections import defaultdict

from apache_beam import Map, PTransform

from ..objects.record import Record

# This should match the value from gpsdio-segment
very_slow = 0.35


def median(iterable):
    seq = sorted(iterable)
    if not seq:
        return None
    quotient, remainder = divmod(len(seq), 2)
    if remainder:
        return seq[quotient]
    return sum(seq[quotient - 1 : quotient + 1]) / 2


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
            speed = median(x.speed for x in records_at_t if x.speed is not None)
            course = math.degrees(
                math.atan2(
                    median(
                        math.sin(math.radians(x.course))
                        for x in records_at_t
                        if x.course is not None
                    ),
                    median(
                        math.cos(math.radians(x.course))
                        for x in records_at_t
                        if x.course is not None
                    ),
                )
            )

            if speed is None:
                continue
            if course is None:
                if speed <= very_slow:
                    course = 0.0
                else:
                    continue

            rcd = Record(
                id=id_,
                timestamp=t,
                lat=median(x.lat for x in records_at_t),
                lon=median(x.lon for x in records_at_t),
                speed=speed,
                course=course,
            )

            sorted_records.append(rcd)
        return key, sorted_records

    def expand(self, xs):
        return xs | Map(self.sort_and_uniquify_by_time)
