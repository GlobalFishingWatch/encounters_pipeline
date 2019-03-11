
from pipeline.create_raw_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, source_dataset="SOURCE_DATASET"):
        self.start_date = start_date
        self.end_date = end_date
        self.source_datasets = [source_dataset]
        self.fast_test = False
        self.vessel_id_column = None
    def view_as(self, x):
      return self


def test_create_queries_1():
    options=DummyOptions("2016-01-01", "2016-01-01")
    assert [x.strip() for x in create_queries(options)] == [x.strip() for x in ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      UNIX_MILLIS(a.timestamp) / 1000.0  AS timestamp,
      CONCAT("", vessel_id) AS id
    FROM
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.position_messages_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20151231' AND '20160101' AND
              lat   IS NOT NULL AND
              lon   IS NOT NULL AND
              speed IS NOT NULL) a
    INNER JOIN
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.segments_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20151231' AND '20160101' AND
        noise = FALSE) b
    USING(_TABLE_SUFFIX, seg_id)
    """]]
    
def test_create_queries_2():
    options=DummyOptions("2012-5-01", "2017-05-15")
    assert [x.strip() for x in create_queries(options)] == [x.strip() for x in ["""
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      UNIX_MILLIS(a.timestamp) / 1000.0  AS timestamp,
      CONCAT("", vessel_id) AS id
    FROM
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.position_messages_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20120430' AND '20150124' AND
              lat   IS NOT NULL AND
              lon   IS NOT NULL AND
              speed IS NOT NULL) a
    INNER JOIN
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.segments_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20120430' AND '20150124' AND
        noise = FALSE) b
    USING(_TABLE_SUFFIX, seg_id)
    """,

        """
    SELECT
      lat        AS lat,
      lon        AS lon,
      speed      AS speed,
      UNIX_MILLIS(a.timestamp) / 1000.0  AS timestamp,
      CONCAT("", vessel_id) AS id
    FROM
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.position_messages_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20150125' AND '20170515' AND
              lat   IS NOT NULL AND
              lon   IS NOT NULL AND
              speed IS NOT NULL) a
    INNER JOIN
      (SELECT *, _TABLE_SUFFIX FROM `SOURCE_DATASET.segments_*` 
        WHERE _TABLE_SUFFIX BETWEEN '20150125' AND '20170515' AND
        noise = FALSE) b
    USING(_TABLE_SUFFIX, seg_id)
    """]]