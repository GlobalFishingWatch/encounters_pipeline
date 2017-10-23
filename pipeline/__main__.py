from pipeline.definition import PipelineDefinition
from pipeline.definition import ConsolidationPipelineDefinition
import apache_beam as beam
import logging
import pipeline.options.parser as parser

def run():
    (options, pipeline_options) = parser.parse()

    definition = PipelineDefinition(options)
    pipeline = definition.build(beam.Pipeline(options=pipeline_options))
    job = pipeline.run()

    if options.remote:
        job.wait_until_finish()

    consolidation_definition = ConsolidationPipelineDefinition(options)
    consolidation_pipeline = consolidation_definition.build(beam.Pipeline(options=pipeline_options))
    consolidation_job = consolidation_pipeline.run()

    if options.remote and options.wait:
        consolidation_job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
