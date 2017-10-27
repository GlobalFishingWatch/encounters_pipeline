from pipeline.options.actions import ReadFileAction

def setup(parser):
    """
    Setup global pipeline options available both on local and remote runs.

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    required = parser.add_argument_group('global required arguments')
    required.add_argument(
        '--source',
        help="BigQuery query that returns the records to process. Might be either a query or a file containing the query if using the `@path/to/file.sql syntax`. See examples/local.sql.",
        required=True,
        action=ReadFileAction,
    )
    parser.add_argument(
        '--raw_sink_write_disposition',
        help='How to merge the output of this process with whatever records are already there in the sink tables. Might be WRITE_TRUNCATE to remove all existing data and write the new data, or WRITE_APPEND to add the new date without. Defaults to WRITE_APPEND.',
        default='WRITE_APPEND',
    )
    parser.add_argument(
        '--postprocess_only',
        help='Skip generating files and only run postprocessing (merge / filter) step',
        action='store_true'
        )
    required = parser.add_argument_group('remote required arguments')
    required.add_argument(
        '--raw_sink',
        help='BigQuery table names to which the raw processed data is uploaded.',
        required=True,
    )