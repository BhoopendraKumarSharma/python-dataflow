import csv
import logging
from io import StringIO
from datetime import datetime
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"


class WriteToGCS(beam.DoFn):
    def process(self, element):
        filename, records = element
        date_str = 'exec_date=' + filename.split('_')[3][:8]
        new_file_path = f"{date_str}/{filename}"
        output_bucket = "gs://df-data-output"
        output_path = output_bucket + '/' + new_file_path
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            for record in records:
                f.write(record['line'].encode('utf-8'))  # write the original line


def parse_csv_bq(element):
    file_name = element[0].metadata.path.split('/')[-1]
    line = element[1]
    if 'cdrRecordType'.lower() not in line.lower() and 'INTEGER'.lower() not in line.lower():
        values = line.split(',')
        record = {
            'filename': file_name,
            'line': line,  # store the original line
            'cdrRecordType': str(values[0]),
            'globalCallID_callManagerId': str(values[1]),
            'globalCallID_callId': str(values[2]),
            'globalCallId_ClusterID': str(values[67]),
            'pkid': str(values[52]),
        }
        return record


def run(argv=None):
    # Hardcode your input parameters here
    input_path = "gs://bks-pipeline-test/cdr*"
    output_table = "vivid-argon-376508:bksdatasets001.dataflow-pipeline-test"
    destination_bucket = "gs://df-data-output/"
    poll_interval = 120
    window_size = 300
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Create File Patterns" >> beam.Create([input_path])

        csv_lines_bq = (
            input_file_patterns
            | "ReadMatches for BQ" >> fileio.ReadMatches(file_pattern=input_path).with_format(fileio.TextFileFormat())
            | "ReadFiles for BQ" >> beam.Map(lambda file: (file, file.read_utf8()))
            | "ParseCSV for BQ" >> beam.FlatMap(parse_csv_bq)
        )

        write_to_BQ = (
                csv_lines_bq
                | "FilterForBQ" >> beam.Filter(
            lambda record: 'line' not in record)  # remove records with original lines
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema='filename:STRING,cdrRecordType:STRING,globalCallID_callManagerId:STRING,globalCallID_callId:STRING,globalCallId_ClusterID:STRING,pkid:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        )

        write_to_GCS = (
                csv_lines_bq
                | "FilterForGCS" >> beam.Filter(
            lambda record: 'line' in record)  # keep only records with original lines
                | "MakePair" >> beam.Map(lambda record: (record['filename'], record))  # create a key-value pair
                | "GroupForGCS" >> beam.GroupByKey()  # group by filename
                | "WriteToGCS" >> beam.ParDo(WriteToGCS())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
