from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("start_time", "TIMESTAMP")
    builder.add("end_time", "TIMESTAMP")
    builder.add("mean_latitude", "FLOAT")
    builder.add("mean_longitude", "FLOAT")
    builder.add("median_distance_km", "FLOAT")
    builder.add("median_speed_knots", "FLOAT")

    for v in [1, 2]:
        builder.add("vessel_{}_id".format(v), "INTEGER")
        builder.add("vessel_{}_point_count".format(v), "INTEGER")

    return builder.schema


