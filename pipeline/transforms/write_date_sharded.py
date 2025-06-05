from pipeline.utils.ver import get_pipe_ver

import apache_beam as beam
import datetime as dt
import pytz


datetimeFromTimestamp = lambda ts: dt.datetime.fromtimestamp(ts, tz=pytz.UTC)

class WriteDateSharded(beam.PTransform):
    def __init__(self, cloud_options, opts, schema, key="end_time"):
        self.source = opts.source_tables
        self.sink = opts.raw_table
        self.max_encounter_dist_km = opts.max_encounter_dist_km
        self.min_encounter_time_minutes = opts.min_encounter_time_minutes
        self.start_date = opts.start_date
        self.end_date = opts.end_date
        assert type(schema), beam.io.gcp.internal.clients.bigquery.TableSchema
        self.schema = beam.io.gcp.bigquery.WriteToBigQuery.get_dict_table_schema(schema)
        self.key = key
        self.write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
        self.ver = get_pipe_ver()

    def get_description(self):
        return {
            "destinationTableProperties": {
                "description": f"""
Created by the encounters_pipeline: {self.ver}.
* Creates raw encounters, reads the data from source and computes encounters over windows between start_date and end_date.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Sources: {self.source}
* Maximum distance for vessels to be elegible (km): {self.max_encounter_dist_km}
* Minimum minutes of vessel adjacency before we have an encounter: {self.min_encounter_time_minutes}
* Date range: {self.start_date}, {self.end_date}
            """,
            },
        }

    def compute_table_for_event(self, msg):
        dt = datetimeFromTimestamp(msg[self.key]).date()
        return f"{self.sink}{dt:%Y%m%d}"

    def expand(self, pcoll):
        return pcoll | "WriteRawEncounters" >> beam.io.gcp.bigquery.WriteToBigQuery(
            self.compute_table_for_event,
            schema=self.schema,
            write_disposition=self.write_disposition,
            additional_bq_parameters=self.get_description(),
        )

