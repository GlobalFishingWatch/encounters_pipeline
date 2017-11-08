
from pipeline.raw_definition import RawPipelineDefinition

class DummyOptions(object):
    def __init__(self, start_date, end_date, source_table="SOURCE_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.source_table = source_table
        self.fast_test = False


def test_create_queries_1():
    defn = RawPipelineDefinition(options=DummyOptions("2016-01-01", "2016-01-01"))
    assert list(defn.create_queries()) == ["""
        SELECT
          lat        AS lat,
          lon        AS lon,
          speed      AS speed,
          FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
          mmsi       AS id
        FROM
          TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                                TIMESTAMP('2015-12-31'), TIMESTAMP('2016-01-01'))
        WHERE
          lat   IS NOT NULL AND
          lon   IS NOT NULL AND
          speed IS NOT NULL
        """]
    
def test_create_queries_2():
    defn = RawPipelineDefinition(options=DummyOptions("2012-5-01", "2017-05-15"))
    assert list(defn.create_queries()) == ["""
        SELECT
          lat        AS lat,
          lon        AS lon,
          speed      AS speed,
          FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
          mmsi       AS id
        FROM
          TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                                TIMESTAMP('2012-04-30'), TIMESTAMP('2015-01-24'))
        WHERE
          lat   IS NOT NULL AND
          lon   IS NOT NULL AND
          speed IS NOT NULL
        """,

        """
        SELECT
          lat        AS lat,
          lon        AS lon,
          speed      AS speed,
          FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
          mmsi       AS id
        FROM
          TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                                TIMESTAMP('2015-01-25'), TIMESTAMP('2017-05-15'))
        WHERE
          lat   IS NOT NULL AND
          lon   IS NOT NULL AND
          speed IS NOT NULL
        """
        ]