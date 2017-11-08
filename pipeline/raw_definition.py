import datetime
from apache_beam import io
from apache_beam import Flatten
from pipeline.objects.record import RecordsFromDicts
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.sort_by_time import SortByTime
from pipeline.transforms.resample import Resample
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.objects.encounter import EncountersToDicts
from pipeline.transforms.writers import WriteToBq


RESAMPLE_INCREMENT_MINUTES = 10.0
MAX_GAP_HOURS = 1.0
MAX_ENCOUNTER_DISTANCE_KM = 1.0
MIN_ENCOUNTER_TIME_MINUTES = 120.0


class RawPipelineDefinition():

    precursor_days = 1

    def __init__(self, options):
        self.options = options

    def create_queries(self):
        template = """
        SELECT
          lat        AS lat,
          lon        AS lon,
          speed      AS speed,
          FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
          mmsi       AS id
        FROM
          TABLE_DATE_RANGE([world-fishing-827:{table}.], 
                                TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}'))
        WHERE
          lat   IS NOT NULL AND
          lon   IS NOT NULL AND
          speed IS NOT NULL
        """
        start_date = datetime.datetime.strptime(self.options.start_date, '%Y-%m-%d') 
        start_window = start_date - datetime.timedelta(days=1)
        end_date= datetime.datetime.strptime(self.options.end_date, '%Y-%m-%d') 
        while start_window <= end_date:
            end_window = min(start_window + datetime.timedelta(days=999), end_date)
            query = template.format(table=self.options.source_table, start=start_window, end=end_window)
            print(query)
            yield query
            start_window = end_window + datetime.timedelta(days=1)

    def build(self, pipeline):
        writer = WriteToBq(
            table=self.options.raw_sink,
            write_disposition="WRITE_APPEND",
        )

        sources = [(pipeline | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                        for (i, x) in enumerate(self.create_queries())]


        (
            sources
            | Flatten()
            | RecordsFromDicts()
            | Resample(increment_s = 60 * RESAMPLE_INCREMENT_MINUTES, 
                       max_gap_s = 60 * 60 * MAX_GAP_HOURS) 
            | ComputeAdjacency(max_adjacency_distance_km=MAX_ENCOUNTER_DISTANCE_KM) 
            | ComputeEncounters(max_km_for_encounter=MAX_ENCOUNTER_DISTANCE_KM, 
                                min_minutes_for_encounter=MIN_ENCOUNTER_TIME_MINUTES) 
            | EncountersToDicts()
            | writer
        )

        return pipeline