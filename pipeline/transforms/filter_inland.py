from apache_beam import PTransform
from apache_beam import Filter
import apache_beam as beam
import os

from .mask import Mask


this_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(this_dir, ".."))


class FilterInland(PTransform):

    def __init__(self):
        self.mask = Mask(os.path.join(parent_dir, "data/sparse_inland.pickle"))

    def not_inland(self, msg):
        if (-90 <= msg.mean_latitude <= 90) and (-180 <= msg.mean_longitude <= 180):
            return not self.mask.query(msg.mean_latitude, msg.mean_longitude) 
        return False

    def expand(self, xs):
        return (
            xs
            | Filter(self.not_inland)
        )
