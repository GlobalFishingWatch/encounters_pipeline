from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("vessel_id", "STRING")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("neighbor_count", "INTEGER")

    return builder.schema