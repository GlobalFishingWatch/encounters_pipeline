import logging
from apache_beam import io
from apache_beam import Pipeline
from apache_beam.runners import PipelineState
from pipeline.objects.encounter import EncountersFromDicts
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.filter_ports import FilterPorts
from pipeline.objects.encounter import EncountersToDicts
from pipeline.options.merge_options import MergeOptions
from pipeline.transforms.writers import WriteToBq



def run(options):

    p = Pipeline(options=options)

    merge_options = options.view_as(MergeOptions)

    if merge_options.merged_sink_table:
        writer_merged = WriteToBq(
            table=merge_options.merged_sink_table,
            write_disposition="WRITE_TRUNCATE",
        )
    else:
        writer_merged = None
    writer_filtered = WriteToBq(
        table=merge_options.sink_table,
        write_disposition="WRITE_TRUNCATE",
    )

    query = """SELECT
        vessel_1_id, vessel_2_id, 
        FLOAT(TIMESTAMP_TO_MSEC(start_time)) / 1000  AS start_time,
        FLOAT(TIMESTAMP_TO_MSEC(end_time)) / 1000    AS end_time,
        mean_latitude, mean_longitude, 
        median_distance_km, median_speed_knots, 
        vessel_1_point_count, vessel_2_point_count
           FROM [{}]
    """.format(merge_options.raw_table)

    merged = (p
        | io.Read(io.gcp.bigquery.BigQuerySource(query=query))
        | EncountersFromDicts()
        | MergeEncounters(min_hours_between_encounters=24) # TODO: parameterize
    )

    if writer_merged is not None:
        (merged 
            | "MergedToDicts" >> EncountersToDicts()
            | "WriteMerged" >> writer_merged
        )

    (merged
        | FilterPorts()
        | "FilteredToDicts" >> EncountersToDicts()
        | "WriteFiltered" >> writer_filtered
    )


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1