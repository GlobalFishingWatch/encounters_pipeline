from apache_beam import PTransform
from apache_beam import FlatMap
from apache_beam import GroupByKey

class Ungroup(PTransform):

    def untag(self, item):
        key, records = item
        return records

    def expand(self, xs):
        return (
            xs
            | FlatMap(self.untag)
        )
