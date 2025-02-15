from google.cloud import bigquery
from pipeline.schemas.output import build as output_schema
from pipeline.utils.ver import get_pipe_ver
import apache_beam as beam
import logging


def list_to_dict(labels):
    return {x.split("=")[0]: x.split("=")[1] for x in labels}


class WriteEncountersToBQ(beam.PTransform):

    def __init__(self, options: dict, cloud_opts: dict):
        self.table = options.sink_table
        self.project = cloud_opts.project
        self.schema = output_schema()
        self.write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
        self.create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        self.vessel_id_tables = options.vessel_id_tables
        self.spatial_measures_table = options.spatial_measures_table
        self.min_hours_between_encounters = options.min_hours_between_encounters
        self.bad_segs_filter = "Yes" if options.bad_segs_table else "No"
        self.ver = get_pipe_ver()
        self.start_date = options.start_date
        self.end_date = options.end_date
        self.labels = list_to_dict(cloud_opts.labels)
        self.bqclient = bigquery.Client(project=self.project)
        dataset_id, table_name = self.table.split(".")
        dataset_ref = bigquery.DatasetReference(self.project, dataset_id)
        self.table_ref = dataset_ref.table(table_name)

    def update_table_description(self):
        table = self.bqclient.get_table(self.table_ref)  # API request
        table.description = f"""
Created by the encounters_pipeline: {self.ver}
* Merges the encounters that are close in time into one long encounter.
* https://github.com/GlobalFishingWatch/encounters_pipeline
* Source raw encounters: {self.table}
* Source vessel id: {self.vessel_id_tables}
* Source Spatial Measure: {self.spatial_measures_table}
* Min hours before encounter: {self.min_hours_between_encounters}
* Skip bad segments table? {self.bad_segs_filter}
* Date range: {self.start_date}, {self.end_date}
        """
        table_updated = self.bqclient.update_table(table, ["description"])  # API request
        assert table_updated.description == table.description
        logging.info(f"Update descriptions to output table <{self.table}>")

    def update_labels(self):
        table = self.bqclient.get_table(self.table_ref)  # API request
        table.labels = self.labels
        self.bqclient.update_table(table, ["labels"])  # API request
        logging.info(f"Update labels to output table <{self.table}>")

    def expand(self, pcoll):
        return pcoll | "WriteEncounters" >> beam.io.WriteToBigQuery(
            self.table,
            schema=self.schema,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            additional_bq_parameters={
                "timePartitioning": {
                    "type": "MONTH",
                    "field": "start_time",
                    "requirePartitionFilter": False,
                },
                "clustering": {"fields": ["start_time"]},
            },
        )
