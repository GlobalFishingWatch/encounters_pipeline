import datetime as dt
import pytz
from pipeline.objects import record
from pipeline.transforms import resample

def Record(timestamp, lat, lon, id=b'0', speed=0.0):
    timestamp = dt.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=pytz.utc)
    return record.Record(id=id, timestamp=timestamp, lat=lat, lon=lon, speed=speed)

def ResampledRecord(timestamp, lat, lon, point_density=1.0, id=b'0', speed=0.0):
    if not isinstance(timestamp, dt.datetime):
        timestamp = dt.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=pytz.utc)
    return resample.ResampledRecord(id=id, timestamp=timestamp, lat=lat, lon=lon, 
                                    speed=speed, point_density=point_density)

input_records = [
    Record("2011-06-30 23:58:00 UTC", lat = 10.0, lon = 10.0),
    # Pick up the exact value at 00:00:00
    Record("2011-07-01 00:00:00 UTC", lat = 10.3, lon = 10.0),
    Record("2011-07-01 00:02:00 UTC", lat = 10.0, lon = 10.0),
    Record("2011-07-01 00:04:00 UTC", lat = 10.0, lon = 10.0),
    Record("2011-07-01 00:06:00 UTC", lat = 10.0, lon = 10.0),
    Record("2011-07-01 00:08:00 UTC", lat = 10.0, lon = 10.0),
    # Interpolate time into 00:10:00, but no movement.
    Record("2011-07-01 00:12:00 UTC", lat = 10.0, lon = 10.0),
    Record("2011-07-01 00:18:00 UTC", lat = 10.0, lon = 10.0),
    # Interpolate into 00:20:00, closer to the 00:18:00 point.
    Record("2011-07-01 00:26:00 UTC", lat = 10.0, lon = 11.0),
    # Do not generate samples where the surrounding points are more than
    # an hour apart.
    Record("2011-07-01 01:38:00 UTC", lat = 10.0, lon = 11.0),
    # Interpolate into 01:40:00.
    Record("2011-07-01 01:42:00 UTC", lat = 11.0, lon = 11.0),
    Record("2011-07-01 02:22:00 UTC", lat = 11.0, lon = 11.0)
]

expected = [
    ResampledRecord("2011-07-01 00:00:00 UTC", 10.3, 10.0, 1.0),
    ResampledRecord("2011-07-01 00:10:00 UTC", 10.0, 10.0, 1.0),
    ResampledRecord("2011-07-01 00:20:00 UTC", 10.0, 10.25, 1.0),
    ResampledRecord("2011-07-01 01:40:00 UTC", 10.5, 11.0, 1.0),
    ResampledRecord("2011-07-01 01:50:00 UTC", 11.0, 11.0, 0.25),
    ResampledRecord("2011-07-01 02:00:00 UTC", 11.0, 11.0, 0.25),
    ResampledRecord("2011-07-01 02:10:00 UTC", 11.0, 11.0, 0.25),
    ResampledRecord("2011-07-01 02:20:00 UTC", 11.0, 11.0, 0.25)
]


def test_resample():
    obj = resample.Resample(increment_s=60 * 10, max_gap_s=60 * 60)
    assert obj.resample_records(input_records) == expected
