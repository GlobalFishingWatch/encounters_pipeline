from pipeline.options import logging_monkeypatch
from pipeline.options import validate_options
from pipeline.options import LoggingOptions

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

