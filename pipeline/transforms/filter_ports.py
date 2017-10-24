from apache_beam import PTransform
from apache_beam import Filter
import apache_beam as beam
import os

from .mask import Mask


this_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(this_dir, ".."))


class FilterPorts(PTransform):

    def __init__(self):
        self.mask = Mask(os.path.join(parent_dir, "dist_to_port_10km.pickle"))

    def not_near_port(self, msg):
        return self.mask.query(msg.mean_latitude, msg.mean_longitude) 

    def expand(self, xs):
        return (
            xs
            | Filter(self.not_near_port)
        )
