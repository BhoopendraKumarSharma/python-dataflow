import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
import os

def transform_function(line):
    # Example transformation function
    # Replace 'foo' with 'bar' in the input line
    return line.replace('foo', 'bar')

def move_files(files):
    # Example function to move processed files to a new subdirectory
    for file in files:
        old_path = file.path
        new_path = os.path.join(os.path.dirname(old_path), 'processed', os.path.basename(old_path))
        file.rename(new_path)

options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).project = 'YOUR_PROJECT_ID'
options.view_as(StandardOptions).region = 'YOUR_REGION'
options.view_as(StandardOptions).temp_location = 'gs://YOUR_BUCKET_NAME/tmp'
options.view_as(StandardOptions).staging_location = 'gs://YOUR_BUCKET_NAME/staging'

gcs_input = 'gs://YOUR_BUCKET_NAME/input/*'

with beam.Pipeline(options=options) as p:
    data = (p | 'ReadData' >> ReadFromText(gcs_input)
              | 'BatchData' >> beam.BatchElements(min_batch_size=1000, max_batch_size=1000)
              | 'TransformData' >> beam.Map(transform_function)
              | 'WriteToBigQuery' >> WriteToBigQuery(
                table='YOUR_BIGQUERY_TABLE_NAME',
                dataset='YOUR_BIGQUERY_DATASET_NAME',
                project='YOUR_PROJECT_ID',
                schema='YOUR_BIGQUERY_TABLE_SCHEMA',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
           )

    # Move processed files to a new subdirectory
    files = data.pipeline | 'ListProcessedFiles' >> beam.io.gcp.gcs.ListObjects(gcs_input)
    files | 'MoveFiles' >> beam.Map(move_files)
