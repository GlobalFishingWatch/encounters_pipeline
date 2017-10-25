from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
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

    def __init__(self, max_adjacency_distance_km):
        self.max_adjacency_distance_km = max_adjacency_distance_km
        assert max_adjacency_distance_km < 2 * APPROX_ENCOUNTERS_S2_SIZE_KM


    def compute_distances(self, records):
        records = list(records)
        # Build up a list of all plausible neighbors using S2ids.
        # A plausible neighbor for a given cell is a vessel in
        # that cell or any surrounding cell.
        s2_to_ndxs = defaultdict(list)
        tagged_records = []
        for i, rcd in enumerate(records):
            cellid = S2CellId(rcd.lat, rcd.lon)
            token = cellid.to_token()
            tagged_records.append((token, rcd))
            s2_to_ndxs[token].append(i)
            for nbrid in cellid.get_all_neighbors(ENCOUNTERS_S2_SCALE):
                s2_to_ndxs[nbrid.to_token()].append(i)

        for i, (token, rcd1) in enumerate(tagged_records):
            closest_dist = self.max_adjacency_distance_km
            closest_nbr = None
            nbr_count = 0
            for j in s2_to_ndxs[token]:
                if i == j:
                    continue
                rcd2 = records[j]
                distance = compute_distance(rcd1, rcd2)
                if distance <= self.max_adjacency_distance_km:
                    nbr_count += 1
                    if distance <= closest_dist:
                        closest_dist = distance
                        closest_nbr = rcd2
            if closest_nbr is None:
                yield (rcd1, None, inf, 0)
            else:
                yield (rcd1, closest_nbr, closest_dist, nbr_count)


    def annotate_adjacency(self, resampled_item):
        time, records = resampled_item
        for rcd1, rcd2, distance, count in self.compute_distances(records):
            if distance <= self.max_adjacency_distance_km:
                yield AnnotatedRecord(neighbor_count = count, 
                                                 closest_neighbor = rcd2, 
                                                 closest_distance = distance,
                                                 **rcd1._asdict())
            else:
                yield AnnotatedRecord(neighbor_count = 0, 
                                                 closest_neighbor = None, 
                                                 closest_distance = inf,
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


