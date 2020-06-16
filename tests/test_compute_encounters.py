import pytest
import unittest
import logging
from collections import namedtuple
from collections import OrderedDict
import datetime
import pytz

import apache_beam as beam
from apache_beam import Map
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from .test_resample import Record
from .test_resample import ResampledRecord
from .series_data import simple_series_data
from .series_data import real_series_data

from pipeline.create_raw_pipeline import ensure_bytes_id
from pipeline.transforms.resample import Resample
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.objects import encounter
from pipeline.transforms.merge_encounters import MergeEncounters


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


inf = float('inf')

def ts(x):
    return datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)


def TaggedAnnotatedRecord(vessel_id, record, neighbor_count, closest_neighbor):
    if closest_neighbor is None:
        nbr_dist = inf
    else:
        nbr_id, nbr_dist, nbr_args = closest_neighbor
        closest_neighbor = ResampledRecord(*nbr_args, id=nbr_id)
    record = record._replace(id=vessel_id)
    return (vessel_id,
        compute_adjacency.AnnotatedRecord(neighbor_count=neighbor_count, closest_distance=nbr_dist,
            record=record, closest_neighbor=closest_neighbor)
        )


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestComputeEncounters(unittest.TestCase):

    def test_simple_encounters(self):
        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(simple_series_data)
                | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
                | Resample(increment_s=60*10, max_gap_s=60*70)
                | ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
            )
            assert_that(results, equal_to(self._get_simple_expected()))

    def test_real_data(self):
        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(real_series_data)
                | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
                | Resample(increment_s=60*10, max_gap_s=60*70)
                | ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
            )
            assert_that(results, equal_to(self._get_real_expected()))

    def test_message_generation(self):
        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(real_series_data)
                | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
                | Resample(increment_s=60*10, max_gap_s=60*70)
                | ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_messages_expected()))

    def test_merge_messages(self):
        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(real_series_data)
                | 'Ensure ID is bytes' >> Map(ensure_bytes_id)
                | Resample(increment_s=60*10, max_gap_s=60*70)
                | ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | ComputeEncounters(max_km_for_encounter=0.5, min_minutes_for_encounter=30)
                | MergeEncounters(min_hours_between_encounters=24)
                | encounter.Encounter.ToDict()
            )
            assert_that(results, equal_to(self._get_merged_expected()))

    def _get_simple_expected(self):
        return [
                    encounter.Encounter(b'1', b'2',
                        ts("2011-01-01T16:10:00Z"),
                        ts("2011-01-01T17:00:00Z"),
                        -1.4719963, 55.21973783333333,
                        0.20333088100150815,
                        0.6467305031568592, 6, 5),
                    encounter.Encounter(b'2', b'1',
                        ts("2011-01-01T16:10:00Z"),
                        ts("2011-01-01T17:00:00Z"),
                        -1.4710235833333334, 55.21933776666667,
                        0.20333088100150815,
                        1.1175558891689739, 5, 6)
        ]



    def _get_real_expected(self):
        return [
            encounter.Encounter(b'441910000', b'563418000', 
                          ts("2015-03-19T07:40:00Z"),
                          ts("2015-03-19T20:10:00Z"),
                          -27.47909444042379, 38.53374945895693,
                          0.028845166034633843,
                          0.20569554161530468, 7, 6),
            encounter.Encounter(b'563418000', b'441910000',
                          ts("2015-03-19T07:40:00Z"),
                          ts("2015-03-19T10:10:00Z"),
                          -27.480823491781422, 38.53562707753466,
                          0.030350584066300215,
                          0.17049202182476167, 4, 5)
        ]


    def _get_messages_expected(self):
        return [
            dict([('start_time', 1426750800.0), 
                  ('end_time', 1426759800.0), 
                  ('mean_latitude', -27.480823491781422), ('mean_longitude', 38.53562707753466), 
                  ('median_distance_km', 0.030350584066300215), 
                  ('median_speed_knots', 0.17049202182476167), 
                  ('vessel_1_point_count', 4), ('vessel_2_point_count', 5), 
                  ('vessel_1_id', b'563418000'), 
                  ('vessel_2_id', b'441910000')]), 
            dict([('start_time', 1426750800.0), 
                  ('end_time', 1426795800.0), 
                  ('mean_latitude', -27.47909444042379), 
                  ('mean_longitude', 38.53374945895693), 
                  ('median_distance_km', 0.028845166034633843), 
                  ('median_speed_knots', 0.20569554161530468), 
                  ('vessel_1_point_count', 7), ('vessel_2_point_count', 6), 
                  ('vessel_1_id', b'441910000'), 
                  ('vessel_2_id', b'563418000')])] 



    def _get_merged_expected(self):
        return [{'median_speed_knots': 0.18809378172003316, 'start_time': 
                 1426750800.0, 'mean_longitude': 38.534062395386556, 
                 'vessel_2_point_count': 10, 'mean_latitude': -27.479382615650064, 
                 'end_time':  1426795800.0, 
                 'median_distance_km': 0.02959787505046703, 'vessel_1_point_count': 12, 
                 'vessel_2_id': b'563418000', 'vessel_1_id': b'441910000'}]
