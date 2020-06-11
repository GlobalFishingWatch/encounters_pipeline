from __future__ import division
from apache_beam import PTransform
from apache_beam import Map
import hashlib

from ..objects.record import Record




class AddEncounterId(PTransform):

    @staticmethod
    def add_id(x):
        # Ensure that timestamps are naive or in UTC
        for dt in [x['start_time'], x['end_time']]:
            assert ((dt.tzinfo is None) or 
                    (dt.tzinfo.utcoffset(dt) is None) or 
                    (dt.tzinfo.utcoffset(dt).total_seconds() == 0)), dt.tzinfo
        text = ("encounter|{x[vessel_1_id]}|{x[vessel_2_id]}|" 
                "{x[start_time]:%Y-%m-%d %H:%M:%S}+00|{x[end_time]:%Y-%m-%d %H:%M:%S}+00").format(x=x)
        x['encounter_id'] = hashlib.md5(text.encode('latin-1')).hexdigest()
        return x



    def expand(self, xs):
        return (
            xs
            | Map(self.add_id)
        )