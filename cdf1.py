import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from datetime import datetime
import csv
from io import StringIO
import logging
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'wmt-cc-datasphere-dev-df.json'


# def parse_csv(line):
#     logging.info(line)
#     csv_reader = csv.reader(StringIO(line), delimiter=',')
#     next(csv_reader)  # skip header row
#     for row in csv_reader:
#         record = {
#             'cdrRecordType': row[0],
#             'globalCallID_callManagerId': row[1],
#             'globalCallID_callId': row[2],
#             'globalCallId_ClusterID': row[67],
#             'pkid': row[52],
#         }
#
#         return record

def parse_csv(line):
    # skip header row
    if 'cdrRecordType' not in line:
        values = line.split(',')
        record = {
            'cdrRecordType': str(values[0]),
            'globalCallID_callManagerId': str(values[1]),
            'globalCallID_callId': str(values[2]),
            'globalCallId_ClusterID': str(values[67]),
            'pkid': str(values[52]),
        }
        return record


def run():
    input_path = "gs://cisco-cdr-dataflow-pipeline-test/cdr_L-SC1_02_202209281219_553540"
    output_table = "wmt-cc-datasphere-dev:ciscocdr.dataflow-pipeline-test"
    output_path = "gs://cisco-cdr-dataflow-pipeline-output/output"
    poll_interval = 60
    window_size = 300

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).project = 'wmt-cc-datasphere-dev'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = 'us-central1'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).job_name = 'cisco-cdr-test-20230405'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).staging_location = 'gs://cisco-cdr-dataflow-pipeline-test/temp02/'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).temp_location = 'gs://cisco-cdr-dataflow-pipeline-test/temp01/'
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
                p | 'Read Text File' >> beam.io.ReadFromText(input_path, skip_header_lines=2)
        )

        parsed_data = (
                lines | 'Parse CSV' >> beam.Map(parse_csv)
        )

        # Write to BigQuery
        parsed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema='cdrRecordType:STRING,globalCallID_callManagerId:STRING,globalCallID_callId:STRING,globalCallId_ClusterID:STRING,pkid:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://cisco-cdr-dataflow-pipeline-test/temp01/',
        )

        # Write to GCS
        parsed_data | 'Write to GCS' >> beam.io.WriteToText(output_path)
    logging.info('Job finished!')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
