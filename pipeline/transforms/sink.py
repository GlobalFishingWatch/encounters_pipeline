from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io
from pipeline.schemas.output import build as output_schema
import ujson as json


class SinkBase(PTransform):

    def encode_datetime(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

    def encode_datetime_fields(self, x):
        for name in ['start_time', 'end_time']:
            x[name] = self.encode_datetime(x[name])
        return x


class BQSink(SinkBase):
    def __init__(self, table=None, write_disposition=None):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):
        big_query_sink = io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=output_schema(),
        )

        return (
            xs
            | Map(self.encode_datetime_fields)
            | io.Write(big_query_sink)
        )


class TextSink(SinkBase):

    def __init__(self, path):
        self.path = path

    def to_json(self, value):
        return json.dumps(value)

    def expand(self, xs):
        return (
            xs
            | Map(self.encode_datetime_fields)
            | Map(self.to_json)
            | io.WriteToText(self.path)
        )
