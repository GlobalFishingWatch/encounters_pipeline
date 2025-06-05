from google.cloud import bigquery
from pipeline.schemas.output import build as output_schema
from pipeline.utils.ver import get_pipe_ver
import apache_beam as beam
import logging

list_to_dict = lambda labels: {x.split('=')[0]:x.split('=')[1] for x in labels}

class WriteEncountersToBQ(beam.PTransform):

    def __init__(self, options, cloud_opts):
        self.bqclient = bigquery.Client(project=cloud_opts.project)
        self.table = options.sink_table
        self.schema = output_schema()
        self.labels = list_to_dict(cloud_opts.labels)
        self.table_description = f"""
Created by the encounters_pipeline: {get_pipe_ver()}
* Merges the encounters that are close in time into one long encounter.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Source raw encounters: {self.table}
* Source vessel id: {options.vessel_id_tables}
* Source Spatial Measure: {options.spatial_measures_table}
* Min hours before encounter: {options.min_hours_between_encounters}
* Skip bad segments table? {"Yes" if options.bad_segs_table else "No"}
* Date range: {options.start_date}, {options.end_date}
        """

    def update_table_metadata(self):
        table = self.bqclient.get_table(self.table)  # API request
        table.description = self.table_description
        table.labels = self.labels
        self.bqclient.update_table(table, ["description", "labels"])  # API request
        logging.info(f"Update table metadata to output table <{self.table}>")

    def expand(self, pcoll):
        return pcoll | "WriteEncounters" >> beam.io.WriteToBigQuery(
            self.table,
            schema=self.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": "start_time",
                    "requirePartitionFilter": False
                },
                "clustering": {
                    "fields": ["start_time"]
                },
            }
        )
