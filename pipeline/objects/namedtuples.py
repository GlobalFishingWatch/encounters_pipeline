import ujson
from collections import namedtuple
from . import jsondict
import apache_beam as beam
from apache_beam import typehints
from apache_beam import PTransform
import datetime
import pytz


epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

def datetime_to_s(x):
    return (x - epoch).total_seconds()

def s_to_datetime(x):
    return epoch + datetime.timedelta(seconds=x)


class DatetimeCoder(beam.coders.Coder):

    def encode(self, value):
        return ujson.dumps((value - epoch).total_seconds())

    def decode(self, value):
        return epoch + datetime.timedelta(seconds=ujson.loads(value))

    def is_deterministic(self):
        return True  

beam.coders.registry.register_coder(datetime.datetime, DatetimeCoder)


def coded_namedtuple(name, fields, timefields):

    cls = namedtuple(name, fields)

    class NamedtupleCoder(beam.coders.Coder):
        """A coder used for reading and writing nametuples to/from json"""

        def encode(self, value):
            replacements = {x: datetime_to_s(getattr(value, x)) for x in timefields}
            return ujson.dumps(value._replace(**replacements))

        def decode(self, value):
            ntuple = cls(*ujson.loads(value))
            replacements = {x: s_to_datetime(getattr(ntuple, x)) for x in timefields}
            return ntuple._replace(**replacements)

        def is_deterministic(self):
            return True  

    beam.coders.registry.register_coder(cls, NamedtupleCoder)
    beam.coders.registry.register_coder(typehints.Tuple[int, typehints.Iterable[cls]], NamedtupleCoder)
    beam.coders.registry.register_coder(typehints.Tuple[typehints.Tuple[float, float], typehints.Iterable[cls]], NamedtupleCoder)


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
            return cls(**x)

        def expand(self, p):
            return p | beam.Map(self.from_dict)


    @typehints.with_input_types(cls)
    @typehints.with_output_types(jsondict.JSONDict)
    class ToDict(beam.PTransform):
        """converts namedtuple to a JSONDict"""

        def to_dict(self, x):
            return jsondict.JSONDict(**x._asdict())

        def expand(self, p):
            return p | beam.Map(self.to_dict)

    return cls, FromTuple, FromDict, ToDict


(Record, RecordsFromTuples, RecordsFromDicts, RecordsToDicts) = coded_namedtuple("Record",
                                ["id", "timestamp", "lat", "lon", "speed"], 
                                ['timestamp'])


(ResampledRecord, ResampledRecordsFromTuples, 
    ResampledRecordsFromDicts, ResampledRecordsToDicts) = coded_namedtuple("ResampledRecord", 
    Record._fields + ('point_density',), ['timestamp'])  

(AnnotatedRecord, AnnotatedRecordsFromTuples, 
 AnnotatedRecordsFromDicts, AnnotatedRecordsToDicts) = coded_namedtuple("AnnotatedRecord",
    ResampledRecord._fields + ("neighbor_count", "closest_neighbor", "closest_distance"), ['timestamp'])

(Encounter, EncountersFromTuples, EncountersFromDicts, EncountersToDicts) = coded_namedtuple(
    "Encounter",
    ["vessel_1_id", "vessel_2_id", "start_time", "end_time", 
     "mean_latitude", "mean_longitude", 
     "median_distance_km", "median_speed_knots", 
     "vessel_1_point_count", "vessel_2_point_count"],
     ['start_time', 'end_time'])




