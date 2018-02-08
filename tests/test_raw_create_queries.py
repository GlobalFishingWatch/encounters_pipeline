
from pipeline.create_raw_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, source_table="SOURCE_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.source_tables = [source_table]
        self.fast_test = False
    def view_as(self, x):
      return self


def test_create_queries_1():
    options=DummyOptions("2016-01-01", "2016-01-01")
    assert list(create_queries(options)) == ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
      CONCAT("", vessel_id) AS id
    FROM
      TABLE_DATE_RANGE([SOURCE_TABLE], 
                            TIMESTAMP('2015-12-31'), TIMESTAMP('2016-01-01'))
    WHERE
      lat   IS NOT NULL AND
      lon   IS NOT NULL AND
      speed IS NOT NULL
    """]
    
def test_create_queries_2():
    options=DummyOptions("2012-5-01", "2017-05-15")
    assert list(create_queries(options)) == ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
      CONCAT("", vessel_id) AS id
    FROM
      TABLE_DATE_RANGE([SOURCE_TABLE], 
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
      CONCAT("", vessel_id) AS id
    FROM
      TABLE_DATE_RANGE([SOURCE_TABLE], 
                            TIMESTAMP('2015-01-25'), TIMESTAMP('2017-05-15'))
    WHERE
      lat   IS NOT NULL AND
      lon   IS NOT NULL AND
      speed IS NOT NULL
    """
        ]