from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class MergeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--raw_table', required=True, 
                            help='Table to pull raw (unmerged) encounters to')
        required.add_argument('--sink_table', 
                            help='Table to write file, merged and filtered encounters to')
        required.add_argument('--start_date', required=True, 
                              help="First date to merge.")
        required.add_argument('--end_date', required=True, 
                            help="Last date (inclusive) to merge.")

        optional.add_argument('--merged_sink_table', 
                            help='Table to write merged, but unfiltered encounters to')
        optional.add_argument('--wait', action='store_true',
                            help='Wait for Dataflow to complete.')

