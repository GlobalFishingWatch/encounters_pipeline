import datetime
import logging
import pytz
from apache_beam import Filter
from apache_beam import Flatten
from apache_beam import io
from apache_beam import Pipeline
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import StandardOptions
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.filter_ports import FilterPorts
from pipeline.transforms.filter_inland import FilterInland
from pipeline.objects.encounter import Encounter
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


    start_date = datetime.datetime.strptime(merge_options.start_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)
    end_date= datetime.datetime.strptime(merge_options.end_date, '%Y-%m-%d').replace(tzinfo=pytz.utc)

    queries = Encounter.create_queries(merge_options.raw_table, start_date, end_date)

    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                    for (i, x) in enumerate(queries)]

    raw_encounters = (sources
        | Flatten()
        | Encounter.FromDict()
    )

    if merge_options.min_encounter_time_minutes is not None:
        raw_encounters = (raw_encounters
            | Filter(lambda x: (x.end_time - x.start_time).total_seconds() / 60.0 > merge_options.min_encounter_time_minutes)
        )

    merged  = (raw_encounters
        | MergeEncounters(min_hours_between_encounters=24) # TODO: parameterize
    )

    if writer_merged is not None:
        (merged 
            | "MergedToDicts" >> Encounter.ToDict()
            | "WriteMerged" >> writer_merged
        )

    (merged
        | FilterPorts()
        | FilterInland()
        | "FilteredToDicts" >> Encounter.ToDict()
        | "WriteFiltered" >> writer_filtered
    )

    result = p.run()

    success_states = set([PipelineState.DONE])

    if merge_options.wait or options.view_as(StandardOptions).runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)
        success_states.add(PipelineState.UNKNOWN)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1

