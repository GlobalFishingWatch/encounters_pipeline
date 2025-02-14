from pipeline.options.validate_options import validate_options
from pipeline.options.logging_options import LoggingOptions
from pipeline.options.merge_options import MergeOptions
from pipeline import merge_pipeline

import sys


def run(args):
    options = validate_options(args=args, option_classes=[LoggingOptions, MergeOptions])

    options.view_as(LoggingOptions).configure_logging()

    return merge_pipeline.run(options)


def main(args):
    sys.exit(run(args))


if __name__ == "__main__":
    main(sys.argv)
