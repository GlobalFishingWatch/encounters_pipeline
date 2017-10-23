from apache_beam.io.gcp.internal.clients import bigquery
from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("id",        "INTEGER")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("lat",       "FLOAT")
    builder.add("lon",       "FLOAT")
    builder.add("speed",     "FLOAT")

    return builder.schema
