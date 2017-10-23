import pytest
import unittest
import logging
from collections import namedtuple

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from .test_resample import Record
from .test_resample import ResampledRecord
from .series_data import simple_series_data
from . import compute_adjacency
from . import resample

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


inf = float('inf')

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
class TestComputeAdjacency(unittest.TestCase):

    def test_without_resampling(self):

        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(simple_series_data)
                | compute_adjacency.ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | beam.FlatMap(lambda (key, value): [(key, x) for x in value])
            )
            assert_that(results, equal_to(self._get_expected(interpolated=False)))

    def test_with_resampling(self):

        with _TestPipeline() as p:
            results = (
                p
                | beam.Create(simple_series_data)
                | resample.Resample(increment_s=60*10, max_gap_s=60*70)
                | compute_adjacency.ComputeAdjacency(max_adjacency_distance_km=1.0) 
                | beam.FlatMap(lambda (key, value): [(key, x) for x in value])
            )
            assert_that(results, equal_to(self._get_expected(interpolated=True)))


    def _get_expected(self, interpolated=True):
        density = 0.5 if interpolated else 1.0
        return [
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 15:40:00 UTC", -1.5032387, 55.2340155),0,None),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 15:50:00 UTC",-1.4869308,55.232743),0,None),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:00:00 UTC",-1.4752579,55.2292189,id=1),1,
                ((2,0.6322538449438863, ("2011-01-01 16:00:00 UTC",-1.469593,55.2287294)))),
            # Encounter start
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:10:00 UTC",-1.4719963,55.2251069),1,
                (2,0.20817755069585123, ("2011-01-01 16:10:00 UTC",-1.471138,55.2267713)))] + ([
                TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:20:00 UTC",-1.471653,55.2224633),1,
                    (2,0.16617515594685828, ("2011-01-01 16:20:00 UTC",-1.4707947,55.22368710000001, density)))
                    ] if interpolated else
                [TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:20:00 UTC",-1.471653,55.2224633),0,None)
            ]) + [
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:30:00 UTC",-1.4718246,55.2199175),1,
                (2,0.17064497317314595, ("2011-01-01 16:30:00 UTC",-1.4704514,55.2206029, density))),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:40:00 UTC",-1.472168,55.2185466),1,
                (2,0.1984842113071651, ("2011-01-01 16:40:00 UTC",-1.4704514,55.218057))),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 16:50:00 UTC",-1.4718246,55.2167839),1,
                (2,0.23162828282668915, ("2011-01-01 16:50:00 UTC",-1.4704514,55.215217))),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:00:00 UTC",-1.4725113,55.2156088),1,
                (2,0.4371321971770557, ("2011-01-01 17:00:00 UTC",-1.4728546,55.2116913))),
            # Encounter end
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439),1,
                (2,0.5816845132999947, ("2011-01-01 17:10:00 UTC",-1.4718246,55.2088509))),
            # TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439),1,None),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:20:00 UTC",-1.4859009,55.2089489),0,None),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:30:00 UTC",-1.4974022,55.2078715),0,None),
            TaggedAnnotatedRecord(1, ResampledRecord("2011-01-01 17:40:00 UTC",-1.5140533,55.2069899),0,None),

            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 15:20:00 UTC", -1.4065933, 55.2350923),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 15:30:00 UTC",-1.4218712,55.2342113),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 15:40:00 UTC",-1.4467621,55.2334282),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 15:50:00 UTC",-1.4623833,55.2310789),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:00:00 UTC",-1.469593,55.2287294),1,
                (1,0.6322538449438863, ("2011-01-01 16:00:00 UTC",-1.4752579,55.2292189))),
            # Encounter Start
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:10:00 UTC",-1.471138,55.2267713),1,
                (1,0.20817755069585123, ("2011-01-01 16:10:00 UTC",-1.4719963,55.2251069)))] + ([
                TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:20:00 UTC",-1.4707947,55.22368710000001, density),1,
                    (1,0.16617515594685828, ("2011-01-01 16:20:00 UTC",-1.471653,55.2224633)))
                    ] if interpolated else []) + [
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:30:00 UTC",-1.4704514,55.2206029, density),1,
                (1,0.17064497317314595, ("2011-01-01 16:30:00 UTC",-1.4718246,55.2199175))),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:40:00 UTC",-1.4704514,55.218057),1,
                (1,0.1984842113071651, ("2011-01-01 16:40:00 UTC",-1.472168,55.2185466))),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 16:50:00 UTC",-1.4704514,55.215217),1,
                (1,0.23162828282668915, ("2011-01-01 16:50:00 UTC",-1.4718246,55.2167839))),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:00:00 UTC",-1.4728546,55.2116913),1,
                (1,0.4371321971770557, ("2011-01-01 17:00:00 UTC",-1.4725113,55.2156088))),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:10:00 UTC",-1.4718246,55.2088509),1,
                (1,0.5816845132999947, ("2011-01-01 17:10:00 UTC",-1.4730263,55.2139439))),
            # Encounter End
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:20:00 UTC",-1.4474487,55.2057165),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:30:00 UTC",-1.4278793,55.2040512),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:40:00 UTC",-1.4084816,55.2036594),0,None),
            TaggedAnnotatedRecord(2, ResampledRecord("2011-01-01 17:50:00 UTC",-1.3998985,55.2037574),0,None)
    ]

