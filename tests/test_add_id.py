import datetime as dt
import pytz
from pipeline.objects import record
from pipeline.transforms import add_id
import pytest

epoch = dt.datetime.utcfromtimestamp(0)


@pytest.mark.parametrize("test_input,expected", [
    ({'vessel_1_id' : '3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63', 
     'vessel_2_id' : 'e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c',
     'start_time' : (dt.datetime(2020, 3, 24, 5, 0, 0) - epoch).total_seconds(),  
     'end_time' : (dt.datetime(2020, 3, 24, 8, 30, 0) - epoch).total_seconds()},
      '0000273eee548f797b78236d2f408a77'),
    ])
def test_add_id(test_input, expected):
    adder = add_id.AddEncounterId()
    encounter_id = adder.add_id(test_input)['encounter_id']
    assert encounter_id == expected
    
