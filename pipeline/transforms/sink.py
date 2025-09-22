import datetime as dt
import logging

from apache_beam import Map, PTransform, io
from apache_beam.transforms.window import TimestampedValue
from google.cloud import bigquery
from pipeline.schemas.adjacency_encounter import adjacency_encounter


def cloud_to_labels(ll): return {x.split("=")[0]: x.split("=")[1] for x in ll}


def get_table(bqclient, project: str, tablename: str):
    dataset_id, table_name = tablename.split(".")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    return bqclient.get_table(table_ref)  # API request


def load_labels(project: str, tablename: str, labels: dict):
    bqclient = bigquery.Client(project=project)
    table = get_table(bqclient, project, tablename)
    table.labels = labels
    bqclient.update_table(table, ["labels"])  # API request
    logging.info(f"Update labels to output table <{table}>")


class AdjacencyEncountersSink(PTransform):
    def __init__(self, table, key="timestamp"):
        self.table = table
        self.key = key

    def expand(self, xs):
        return xs | io.WriteToBigQuery(
            table=self.table,
            schema=adjacency_encounter,
            write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=io.BigQueryDisposition.CREATE_NEVER,
        )
