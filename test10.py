import csv
import logging
from io import StringIO
from datetime import datetime
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.transforms.util import Wait
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"


class ProcessFileDoFn(beam.DoFn):
    """A DoFn that processes a file and returns the contents as JSON."""

    def process(self, element):
        """Reads a file, processes its contents, and returns them as JSON.

        Args:
            element: A MatchResult object representing the metadata for a file.

        Returns:
            A list containing a tuple of the form (new_file_path, file_content, processed_data).
        """
        file_path = element.path
        orig_file_path = file_path
        file_name = file_path.split("/")[-1]
        date_str = 'exec_date=' + file_name.split('_')[3][:8]
        new_file_path = f"{date_str}/{file_name}"
        processed_records = []
        output_bucket = "gs://df-data-output"
        output_path = output_bucket + '/' + new_file_path
        with beam.io.gcsio.GcsIO().open(file_path, 'r') as f:
            # Read the lines from the file
            file_content = f.read().decode('utf-8')
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            # write the file to the destination path
            f.write(file_content.encode('utf-8'))
            # To delete the file from source location; it will fail if permission is not granted for delete
            beam.io.gcsio.GcsIO().delete(orig_file_path)

        return new_file_path


class WriteToGCS(beam.DoFn):
    def process(self, element):
        filename, records = element
        date_str = 'exec_date=' + filename.split('_')[3][:8]
        new_file_path = f"{date_str}/{filename}"
        output_bucket = "gs://df-data-output"
        output_path = output_bucket + '/' + new_file_path
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            for record in records:
                line = ','.join(str(value) for value in record.values()) + '\n'
                f.write(line.encode('utf-8'))


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
    content = element[1]
    return (file_name, content)  # return a tuple where the first element is the key


def file_prefix(file_metadata):
    logging.info(f"metadata: {file_metadata}")
    logging.info(f"metadata.path: {file_metadata.metadata.path}")
    original_file_name = file_metadata.metadata.path.split("/")[-1]
    date_str = 'exec_date=' + original_file_name.split('_')[3][:8]
    return f"{date_str}/{original_file_name}"


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
                | "ReadMatches" >> fileio.ReadMatches()
                | "ReadLines" >> beam.Map(lambda file: (file, file.read_utf8_line()))
                | "ParseCSV" >> beam.Map(parse_csv_bq)
        )
        csv_lines_grouped = (
                input_file_patterns
                | "ReadMatches" >> fileio.ReadMatches()
                | "ReadLines" >> beam.Map(lambda file: (file, file.read_utf8_line()))
                | "ParseCSV for Group" >> beam.Map(parse_csv_gcs)
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

        csv_lines_grouped = csv_lines_grouped | "Group by filename" >> beam.GroupByKey()
        csv_lines_grouped | "WriteToGCS" >> beam.ParDo(WriteToGCS())


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
