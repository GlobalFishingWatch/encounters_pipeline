from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
import bisect
import logging
import math
from collections import defaultdict
from ..objects.annotated_record import AnnotatedRecord

import s2sphere

inf = float("inf")

ENCOUNTERS_S2_SCALE = 12
APPROX_ENCOUNTERS_S2_SIZE_KM = 2.0

EARTH_RADIUS_KM = 6371

def compute_distance(rcd1, rcd2):      
    h = ( math.sin(0.5 * math.radians(rcd2.lat - rcd1.lat)) ** 2
        + math.cos(math.radians(rcd1.lat)) * math.cos(math.radians(rcd2.lat)) * 
            math.sin(0.5 * math.radians(rcd2.lon - rcd1.lon)) ** 2)
    h = min(h, 1)
    return  2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))

def S2CellId(lat, lon):
        ll = s2sphere.LatLng.from_degrees(lat, lon)
        cellid = s2sphere.CellId.from_lat_lng(ll)
        cellid = cellid.parent(ENCOUNTERS_S2_SCALE)
        return cellid


class ComputeAdjacency(PTransform):

    def __init__(self, max_adjacency_distance_km, max_tracked_distances=100):
        self.max_adjacency_distance_km = max_adjacency_distance_km
        self.max_tracked_distances = max_tracked_distances
        assert max_adjacency_distance_km < 2 * APPROX_ENCOUNTERS_S2_SIZE_KM

    def compute_distances(self, records):
        records = list(records)
        # Build up a list of all plausible neighbors using S2ids.
        # A plausible neighbor for a given cell is a vessel in
        # that cell or any surrounding cell.
        s2_to_ndxs = defaultdict(list)
        s2_to_tokens = {}
        tagged_records = []
        for i, rcd in enumerate(records):
            cellid = S2CellId(rcd.lat, rcd.lon)
            if cellid not in s2_to_tokens:
                s2_to_tokens[cellid] = tokens = [cellid.to_token()]
                for nbrid in cellid.get_all_neighbors(ENCOUNTERS_S2_SCALE):
                    tokens.append(nbrid.to_token())
            tokens = s2_to_tokens[cellid]
            tagged_records.append((tokens[0], rcd))
            for tkn in tokens:
                s2_to_ndxs[tkn].append(i)

        for i, (token, rcd1) in enumerate(tagged_records):
            closest_dists = []
            closest_nbrs = []
            for j in s2_to_ndxs[token]:
                if i == j:
                    continue
                rcd2 = records[j]
                distance = compute_distance(rcd1, rcd2)
                if distance <= self.max_adjacency_distance_km:
                    ndx = bisect.bisect_right(closest_dists, distance)
                    closest_dists.insert(ndx, distance)
                    closest_nbrs.insert(ndx, rcd2)
                    if len(closest_dists) > self.max_tracked_distances:
                        closest_dists = closest_dists[:self.max_tracked_distances]
                        closest_nbrs = closest_nbrs[:self.max_tracked_distances]
            yield (rcd1, closest_nbrs, closest_dists)

    def annotate_adjacency(self, resampled_item):
        time, records = resampled_item
        for rcd1, neighbors, distances in self.compute_distances(records):
            yield AnnotatedRecord(closest_neighbors = neighbors, 
                                  closest_distances = distances,
                                  **rcd1._asdict())

    def tag_with_time(self, item):
        return (item.timestamp, item)


    def expand(self, xs):
        return (
            xs
            | Map(self.tag_with_time)
            | "Group by time" >> GroupByKey()
            | FlatMap(self.annotate_adjacency)
        )


