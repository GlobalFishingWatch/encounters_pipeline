import apache_beam as beam
import datetime as dt
from pipeline.utils.ver import get_pipe_ver
import pytz


datetimeFromTimestamp = lambda ts: dt.datetime.fromtimestamp(ts, tz=pytz.UTC)

table_description = lambda write_opts: {
    "destinationTableProperties": {
        "description": f"""
        Created by the encounters_pipeline: {write_opts.ver}.
        * Creates raw encounters, reads the data from source and computes encounters over windows between start_date and end_date.
        * https://github.com/GlobalFishingWatch/encounters_pipeline
        * Source: {write_opts.source_table}
        * Maximum distance for vessels to be elegible (km): {write_opts.max_encounter_dist_km}
        * Minimum minutes of vessel adjacency before we have an encounter: {write_opts.min_encounter_time_minutes}
        * Date range: {write_opts.start_date.strftime('%Y-%m-%d')},{write_opts.end_date.strftime('%Y-%m-%d')}
    """,
    },
}

class WriteDateSharded(beam.PTransform):
    def __init__(self, cloud_options, opts, schema, key="end_time"):
        self.project = cloud_options.project
        self.temp_gcs_location = cloud_options.temp_gcs_location
        self.source = opts.source_table
        self.sink = opts.raw_table
        self.max_encounter_dist_km = opt.max_encounter_dist_km
        self.min_encounter_time_minutes = opt.min_encounter_time_minutes
        self.schema = schema
        self.key = key
        self.ver = get_pipe_ver()

    def compute_table_for_event(self, msg):
        dt = datetimeFromTimestamp(msg[self.key]).date()
        return f"{self.project}:{self.sink}{dt:%Y%m%d}"

    def expand(self, pcoll):
        return pcoll | beam.io.WriteToBigQuery(
            self.compute_table_for_event,
            schema=self.schema,
            write_disposition="WRITE_TRUNCATE",
            temp_gcs_location=self.temp_gcs_location,
            additional_bq_parameters=table_description(self),
        )

