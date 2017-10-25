from pipeline.raw_definition import RawPipelineDefinition
from pipeline.merge_definition import MergePipelineDefinition
import apache_beam as beam
import logging
import pipeline.options.parser as parser


def run():
    (options, pipeline_options) = parser.parse()

    definition = RawPipelineDefinition(options)
    pipeline = definition.build(beam.Pipeline(options=pipeline_options))
    job = pipeline.run()

    if options.remote:
        job.wait_until_finish()

    merge_definition = MergePipelineDefinition(options)
    merge_pipeline = merge_definition.build(beam.Pipeline(options=pipeline_options))
    merge_job = merge_pipeline.run()

    if options.remote and options.wait:
        merge_job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
