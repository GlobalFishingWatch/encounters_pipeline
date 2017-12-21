# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipeline.options.merge_options import MergeOptions

from apache_beam.options.pipeline_options import PipelineOptions


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, MergeOptions])

    options.view_as(LoggingOptions).configure_logging()

    from pipeline import merge_pipeline

    merge_pipeline.run(options)

    if options.view_as(MergeOptions).wait: 
        job.wait_until_finish()

if __name__ == '__main__':
    import sys
    sys.exit(run(args=sys.argv))






