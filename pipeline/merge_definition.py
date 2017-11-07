from apache_beam import io
from pipeline.objects.encounter import EncountersFromDicts
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.filter_ports import FilterPorts
from pipeline.objects.encounter import EncountersToDicts
from pipeline.transforms.writers import WriteToBq


class MergePipelineDefinition():

    def __init__(self, options):
        self.options = options

    def build(self, pipeline):

        if self.options.local:
            writer_merged = io.WriteToText('output/encounters_merged')
            writer_filtered = io.WriteToText('output/encounters_filtered')
        elif self.options.remote:
            if self.options.merged_sink:
                writer_merged = WriteToBq(
                    table=self.options.merged_sink,
                    write_disposition=self.options.sink_write_disposition,
                )
            else:
                writer_merged = None
            writer_filtered = WriteToBq(
                table=self.options.sink,
                write_disposition=self.options.sink_write_disposition,
            )

        query = """SELECT
            vessel_1_id, vessel_2_id, 
            FLOAT(TIMESTAMP_TO_MSEC(start_time)) / 1000  AS start_time,
            FLOAT(TIMESTAMP_TO_MSEC(end_time)) / 1000    AS end_time,
     mean_latitude, mean_longitude, 
     median_distance_km, median_speed_knots, 
     vessel_1_point_count, vessel_2_point_count
        FROM [{}]
        """.format(self.options.raw_sink)

        merged = (
            pipeline
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

        return pipeline