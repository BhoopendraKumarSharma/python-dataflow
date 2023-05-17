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
                f.write(record.encode('utf-8'))


def parse_csv_bq(element):
    file_name = element[0].metadata.path.split('/')[-1]
    line = element[1]
    # skip header row
    if 'cdrRecordType'.lower() not in line.lower() and 'INTEGER'.lower() not in line.lower():
        values = line.split(',')
        record = {
            'filename': file_name,
            'cdrRecordType': str(values[0]),
            'globalCallID_callManagerId': str(values[1]),
            'globalCallID_callId': str(values[2]),
            'globalCallId_ClusterID': str(values[67]),
            'pkid': str(values[52]),
        }
        return record


def parse_csv_gcs(element):
    file_name = element[0].metadata.path.split('/')[-1]
    content = element[1].read_utf8()  # read entire file content
    return (file_name, content) # return a tuple where the first element is the key


def run(argv=None):
    # Hardcode your input parameters here
    input_path = "gs://bks-pipeline-test/cdr*"
    output_table = "vivid-argon-376508:bksdatasets001.dataflow-pipeline-test"
    destination_bucket = "gs://df-data-output/"

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Match All New Files" >> fileio.MatchContinuously(f"gs://bks-pipeline-test/*",
                                                                                    interval=60.0)

        csv_lines_bq = (
                input_file_patterns
                | "ReadMatches for BQ" >> fileio.ReadMatches()
                | "ReadLines for BQ" >> beam.Map(lambda file: (file, file.read_utf8()))
                | "ParseCSV for BQ" >> beam.FlatMap(parse_csv_bq)
        )

        csv_lines_grouped = (
                input_file_patterns
                | "ReadMatches for GCS" >> fileio.ReadMatches()
                | "ParseCSV for GCS" >> beam.Map(parse_csv_gcs)
                | "Group by filename" >> beam.GroupByKey()
        )

        write_to_BQ = (
                csv_lines_bq
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema='filename:STRING,cdrRecordType:STRING,globalCallID_callManagerId:STRING,globalCallID_callId:STRING,globalCallId_ClusterID:STRING,pkid:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        )

        csv_lines_grouped | "WriteToGCS" >> beam.ParDo(WriteToGCS())


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
