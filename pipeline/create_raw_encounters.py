# Suppress a spurious warning that happens when you import apache_beam
from pipe_tools.beam import logging_monkeypatch
from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from pipeline.options.create_options import CreateOptions

from apache_beam.options.pipeline_options import PipelineOptions


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, CreateOptions])

    options.view_as(LoggingOptions).configure_logging()

    from pipeline import create_raw_pipeline

    return create_raw_pipeline.run(options)


if __name__ == '__main__':
    import sys
    sys.exit(run(args=sys.argv))






