from apache_beam import io
from apache_beam import Flatten
from pipeline.objects.record import RecordsFromDicts
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.sort_by_time import SortByTime
from pipeline.transforms.resample import Resample
from pipeline.transforms.compute_adjacency import ComputeAdjacency
from pipeline.transforms.compute_encounters import ComputeEncounters
from pipeline.objects.encounter import EncountersToDicts
from pipeline.transforms.writers import WriteToBq


class RawPipelineDefinition():
    def __init__(self, options):
        self.options = options

    def build(self, pipeline):
        writer = WriteToBq(
            table=self.options.raw_sink,
            write_disposition=self.options.raw_sink_write_disposition,
        )

        sources = [(pipeline | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                        for (i, x) in enumerate(self.options.source.split('\n\n')) if x.strip()]


        (
            sources
            | Flatten()
            | RecordsFromDicts()
            | Resample(increment_s = 60 * 10, max_gap_s = 60 * 60 * 1) # TODO: parameterize
            | ComputeAdjacency(max_adjacency_distance_km=1.0) # TOD: parameterize
            | ComputeEncounters(max_km_for_encounter=2, min_minutes_for_encounter=120) # TOD: parameterize
            | EncountersToDicts()
            | writer
        )

        return pipeline