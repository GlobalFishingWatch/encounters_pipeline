from pipeline.transforms.source import Source
from pipeline.transforms.create_records import CreateRecords
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.sort_by_time import SortByTime
from pipeline.transforms.resample import Resample
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.transforms.ungroup import Ungroup
from pipeline.transforms.create_messages import CreateMessages
from pipeline.transforms.sink import BQSink, TextSink


class PipelineDefinition():
    def __init__(self, options):
        self.options = options

    def build(self, pipeline):
        sink = BQSink(
            table=self.options.raw_sink,
            write_disposition=self.options.raw_sink_write_disposition,
        )

        (
            pipeline
            | Source(self.options.source)
            | CreateRecords()

            | GroupById()
            | SortByTime()
            | Resample(increment_s = 60 * 10, max_gap_s = 60 * 60 * 1) # TODO: parameterize

            | ComputeAdjacency(max_adjacency_distance_km=1.0) # TOD: parameterize

            | ComputeEncounters(max_km_for_encounter=2, min_minutes_for_encounter=120) # TOD: parameterize
            | CreateMessages()
            | sink
        )

        return pipeline


from pipeline.transforms.source import EncounterSource
from pipeline.transforms.create_encounter_records import CreateEncounterRecords
from pipeline.transforms.merge_encounters import MergeEncounters
from pipeline.transforms.filter_ports import FilterPorts


class ConsolidationPipelineDefinition():
    def __init__(self, options):
        self.options = options

    def build(self, pipeline):
        if self.options.local:
            sink = TextSink(
                path = 'output/encounters'
            )
        elif self.options.remote:
            sink = BQSink(
                table=self.options.sink,
                write_disposition=self.options.sink_write_disposition,
            )

        query = "SELECT * FROM [{}]".format(self.options.raw_sink)

        (
            pipeline
            | EncounterSource(query)
            | CreateEncounterRecords()
            | MergeEncounters(min_hours_between_encounters=24) # TODO: parameterize
            | FilterPorts()
            | CreateMessages()
            | sink
        )

        return pipeline
