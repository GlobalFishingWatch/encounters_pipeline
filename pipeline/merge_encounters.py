# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipeline.options.merge_options import MergeOptions

from apache_beam.options.pipeline_options import PipelineOptions

import sys


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, MergeOptions])

    options.view_as(LoggingOptions).configure_logging()

    from pipeline import merge_pipeline

    return merge_pipeline.run(options)


def main(args):
    sys.exit(run(args))

if __name__ == '__main__':
    main(sys.argv)

