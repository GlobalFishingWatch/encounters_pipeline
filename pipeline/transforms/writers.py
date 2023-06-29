from apache_beam import io
from pipeline.schemas.output import build as output_schema
from pipeline.utils.ver import get_pipe_ver

table_description = lambda write_opts: f"""
    Created by the encounters_pipeline: {get_pipe_ver()}.
    * Merges the encounters that are close in time into one long encounter.
    * https://github.com/GlobalFishingWatch/encounters_pipeline
    * Source: {write_opts.source_table}
    * Maximum distance for vessels to be elegible (km): {write_opts.max_encounter_dist_km}
    * Minimum minutes of vessel adjacency before we have an encounter: {write_opts.min_encounter_time_minutes}
    * Date range: {write_opts.start_date.strftime('%Y-%m-%d')},{write_opts.end_date.strftime('%Y-%m-%d')}
"""

class WriteEncountersToBQ(io.WriteToBigQuery):

    def __init__(self, options:dict):
        super(io.WriteToBigQuery, self).__init__(
            options.sink_table,
            project=options.project,
            schema=output_schema(),
            write_disposition=io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": "start_time",
                    "requirePartitionFilter": True
                },
                "clustering": {
                    "fields": ["start_time"]
                },
                "destinationTableProperties": {
                    "description": table_desc(options),
                },
            }
        )


