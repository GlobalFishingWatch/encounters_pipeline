from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("encounter_id", "STRING")
    builder.add("start_time", "TIMESTAMP")
    builder.add("end_time", "TIMESTAMP")
    builder.add("mean_latitude", "FLOAT")
    builder.add("mean_longitude", "FLOAT")
    builder.add("median_distance_km", "FLOAT")
    builder.add("median_speed_knots", "FLOAT")
    builder.add("start_lat", "FLOAT")
    builder.add("start_lon", "FLOAT")
    builder.add("end_lat", "FLOAT")
    builder.add("end_lon", "FLOAT")
    for v in [1, 2]:
        builder.add(f"vessel_{v}_id", "STRING")
        builder.add(f"vessel_{v}_seg_ids", "STRING", mode="REPEATED")
        builder.add(f"vessel_{v}_point_count", "INTEGER")

    return builder.schema


    builder.add("events", mode="REPEATED", 
        schema_type=build_port_event_schema().fields,
        description="sequence of port events that occurred during visit"
    )


def build_raw_encounter():

    builder = SchemaBuilder()

    builder.add("encounter_id", "STRING")
    builder.add("start_time", "TIMESTAMP")
    builder.add("end_time", "TIMESTAMP")
    builder.add("mean_latitude", "FLOAT")
    builder.add("mean_longitude", "FLOAT")
    builder.add("median_distance_km", "FLOAT")
    builder.add("median_speed_knots", "FLOAT")
    builder.add("start_lat", "FLOAT")
    builder.add("start_lon", "FLOAT")
    builder.add("end_lat", "FLOAT")
    builder.add("end_lon", "FLOAT")
    for v in [1, 2]:
        builder.add(f"vessel_{v}_seg_id", "STRING")
        builder.add(f"vessel_{v}_point_count", "INTEGER")

    return builder.schema
