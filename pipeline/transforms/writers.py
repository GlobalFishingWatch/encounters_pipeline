from google.cloud import bigquery
from pipeline.schemas.output import build as output_schema
import apache_beam as beam
import logging

from typing import Any

list_to_dict = lambda labels: {x.split('=')[0]:x.split('=')[1] for x in labels}


class WriteEncountersToBQ(beam.PTransform):

    def __init__(
        self,
        table_id: str,
        cloud_opts,
        description: str = None,
        **kwargs: Any,
    ):
        self.bqclient = bigquery.Client(project=cloud_opts.project)
        self.table_id = table_id
        self.schema = output_schema()
        if cloud_opts.labels is None:
            self.labels = {}

        self.labels = list_to_dict(cloud_opts.labels or "")
        self.description = description
        self.kwargs = kwargs

    def update_table_metadata(self):
        table = self.bqclient.get_table(self.table_id)  # API request
        if self.description is not None:
            table.description = self.description

        table.labels = self.labels
        self.bqclient.update_table(table, ["description", "labels"])  # API request
        logging.info(f"Update table metadata to output table <{self.table_id}>")

    def expand(self, pcoll):
        return pcoll | "WriteEncounters" >> beam.io.WriteToBigQuery(
            self.table_id,
            schema=self.schema,
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
            },
            **self.kwargs,
        )
