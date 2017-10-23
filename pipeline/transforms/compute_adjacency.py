from apache_beam import PTransform
from apache_beam import Map
from apache_beam import FlatMap
from apache_beam import GroupByKey
import logging
import math
from collections import defaultdict
from collections import namedtuple

import s2sphere

inf = float("inf")

ANCHORAGES_S2_SCALE = 12
APPROX_ANCHORAGE_S2_SIZE_KM = 2.0

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
        cellid = cellid.parent(ANCHORAGES_S2_SCALE)
        return cellid

        
AnnotatedRecord = namedtuple("AnnotatedRecord",
    ["neighbor_count", "closest_neighbor", "closest_distance", "record"])

class ComputeAdjacency(PTransform):

    def __init__(self, max_adjacency_distance_km):
        self.max_adjacency_distance_km = max_adjacency_distance_km
        assert max_adjacency_distance_km < 2 * APPROX_ANCHORAGE_S2_SIZE_KM



    def compute_distance_map(self, records, max_distance_km):
        """

        returns:

            dict[id1] -> [(id2, distance)]

        where distance < max_distance
        """
        records = list(records)
        # Build up a list of all plausible neighbors using S2ids.
        # A plausible neighbor for a given cell is a vessel in
        # that cell or any surrounding cell.
        s2_to_ndxs = defaultdict(list)
        for i, rcd in enumerate(records):
            cellid = S2CellId(rcd.lat, rcd.lon)
            s2_to_ndxs[cellid.to_token()].append(i)
            for nbrid in cellid.get_all_neighbors(ANCHORAGES_S2_SCALE):
                s2_to_ndxs[nbrid.to_token()].append(i)

        dist_map = defaultdict(list)
        for i, rcd1 in enumerate(records):
            cellid = S2CellId(rcd1.lat, rcd1.lon)
            for j in s2_to_ndxs[cellid.to_token()]:
                if j <= i:
                    # Records is iterable so cant use records[i+1:]
                    continue
                rcd2 = records[j]
                distance = compute_distance(rcd1, rcd2)
                if distance <= max_distance_km:
                    dist_map[rcd1.id].append((distance, rcd2))
                    dist_map[rcd2.id].append((distance, rcd1))
        return dist_map

    def compute_distance_map_x(self, records, max_distance_km):
        """

        returns:

            dict[id1] -> [(id2, distance)]

        where distance < max_distance
        """
        # First try, just use brute force
        # Can be sped using S2 cell / dictionary magic
        dist_map = defaultdict(list)
        for i, rcd1 in enumerate(records):
            for j, rcd2 in enumerate(records):
                if j <= i:
                    # Records is iterable so cant use records[i+1:]
                    continue
                distance = compute_distance(rcd1, rcd2)
                if distance <= max_distance_km:
                    dist_map[rcd1.id].append((distance, rcd2))
                    dist_map[rcd2.id].append((distance, rcd1))
        return dist_map


    def annotate_adjacency(self, resampled_item):
        time, records = resampled_item

        # logging.debug('making id_to_record')

        id_to_record = {x.id: x for x in records}

        distance_map = self.compute_distance_map(records, self.max_adjacency_distance_km) 

        annotated = []
        for id1, rcd1 in id_to_record.items():
            if id1 in distance_map:
                distance, rcd2 = min(distance_map[id1])
                annotated.append(AnnotatedRecord(neighbor_count = len(distance_map[id1]), 
                                                 closest_neighbor = rcd2, 
                                                 closest_distance = distance,
                                                 record = rcd1))
            else:
                annotated.append(AnnotatedRecord(neighbor_count = 0, 
                                                 closest_neighbor = None, 
                                                 closest_distance = inf,
                                                 record = rcd1))
        return time, annotated

    def tag_with_id(self, item):
        return (item.record.id, item)

    def tag_with_time(self, item):
        return (item.timestamp, item)

    def untag(self, item):
        key, value = item
        return value

    def sort_by_time(self, item):
        key, value = item
        value = list(value)
        value.sort(key=lambda x: x.record.timestamp)
        return key, value

    def expand(self, xs):
        return (
            xs
            | "Ungroup by id" >> FlatMap(self.untag)
            | Map(self.tag_with_time)
            | "Group by time" >> GroupByKey()
            | Map(self.annotate_adjacency)
            | "Ungroup by time" >> FlatMap(self.untag)
            | Map(self.tag_with_id)
            | "Group by id" >> GroupByKey()
            | Map(self.sort_by_time)
        )


