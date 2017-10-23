import ujson
from collections import namedtuple
import apache_beam as beam
from apache_beam import typehints
from apache_beam import PTransform


def coded_namedtuple(name, fields):

    cls = namedtuple(name, fields)

    class NamedtupleCoder(beam.coders.Coder):
        """A coder used for reading and writing nametuples to/from json"""

        def encode(self, value):
            return ujson.dumps(value)

        def decode(self, value):
            return cls(ujson.loads(value))

        def is_deterministic(self):
            return True  

    beam.coders.registry.register_coder(cls, NamedtupleCoder)

    @typehints.with_input_types(typehints.Tuple)
    @typehints.with_output_types(cls)
    class FromTuple(beam.PTransform):
        """converts a tuple to a namedtuple"""

        def from_tuple(self, x):
            return cls(*x)

        def expand(self, p):
            return p | beam.Map(self.from_tuple)


    @typehints.with_input_types(typehints.Dict)
    @typehints.with_output_types(cls)
    class FromDict(beam.PTransform):
        """converts a Dict to a namedtuple"""

        def from_dict(self, x):
            return cls(**x._asdict())

        def expand(self, p):
            return p | beam.Map(self.from_dict)

    return cls, FromTuple, FromDict




class CreateRecords(PTransform):

    def create_record(self, msg):
        return Record(**msg)

    def expand(self, xs):
        return (
            xs
            | Map(self.create_record)
        )



(Record, RecordFromTuple, RecordFromDict) = coded_namedtuple("Record",
                                ["id", "timestamp", "lat", "lon", "speed"])



