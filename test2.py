import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class Convert2UpperCase(beam.DoFn):
    def process(self, element):
        return [element.upper()]


options = PipelineOptions()
with beam.Pipeline(options=options) as pipeline:
    files = pipeline | 'List files' >> beam.io.gcp.gcsio.ListObjects(
        'gs://your-bucket/*'
    )
    lines = (
            files
            | 'ReadAllFiles' >> beam.io.ReadAllFromText()
            | 'Convert2UpperCase' >> beam.ParDo(Convert2UpperCase())
    )
    lines | 'WriteToGCS' >> beam.io.WriteToText(
        'gs://your-bucket/output',
        file_name_suffix='.txt'
    )
