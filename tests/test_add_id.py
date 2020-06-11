import datetime as dt
import pytz
from pipeline.objects import record
from pipeline.transforms import add_id
import pytest


input_records = [
    {'vessel_1_id' : '3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63', 'vessel_2_id' : 'e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c',
     'start_time' : dt.datetime(2020, 3, 24, 5, 0, 0),  'end_time' : dt.datetime(2020, 3, 24, 8, 30, 0)},

    {'vessel_1_id' : '3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63', 'vessel_2_id' : 'e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c',
     'start_time' : dt.datetime(2020, 3, 24, 5, 0, 0, tzinfo=pytz.utc),  'end_time' : dt.datetime(2020, 3, 24, 8, 30, 0, tzinfo=pytz.utc)},

]

expected = [
    '0000273eee548f797b78236d2f408a77',
    '0000273eee548f797b78236d2f408a77',
]

@pytest.mark.parametrize("test_input,expected", [
    ({'vessel_1_id' : '3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63', 'vessel_2_id' : 'e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c',
      'start_time' : dt.datetime(2020, 3, 24, 5, 0, 0),  'end_time' : dt.datetime(2020, 3, 24, 8, 30, 0)}, 
      '0000273eee548f797b78236d2f408a77'),
    ({'vessel_1_id' : '3f2b89cd6-6b36-e89c-dfaa-5b9e32703c63', 'vessel_2_id' : 'e5e1847ca-a61f-fbf7-1f79-7c7ed7912b5c',
      'start_time' : dt.datetime(2020, 3, 24, 5, 0, 0),  'end_time' : dt.datetime(2020, 3, 24, 8, 30, 0)}, 
      '0000273eee548f797b78236d2f408a77')
    ])
def test_add_id(test_input, expected):
    encounter_id = add_id.AddEncounterId.add_id(test_input)['encounter_id']
    assert encounter_id == expected
    
