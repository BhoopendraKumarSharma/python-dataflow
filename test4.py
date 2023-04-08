import logging
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import json
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"
PROJECT_ID = "vivid-argon-376508"
DATASET_ID = "bksdatasets001"
TABLE_ID = "dataflow-pipeline-test"
schema = [
    bigquery.SchemaField('cdrRecordType', 'STRING'),
    bigquery.SchemaField('globalCallID_callManagerId', 'STRING'),
    bigquery.SchemaField('globalCallID_callId', 'STRING'),
    bigquery.SchemaField('globalCallId_ClusterID', 'STRING'),
    bigquery.SchemaField('pkid', 'STRING'),
]


def create_new_file_path(file_metadata):
    original_file_name = file_metadata.path.split("/")[-1]
    logging.info('original_file_name>>>>>>>>>>>>>' + original_file_name)
    date_str = 'exec_date=' + original_file_name.split('_')[3][:8]
    new_file_path = f"{date_str}/{original_file_name}"
    logging.info('new_file_path>>>>>>>>>>>>>' + new_file_path)
    return new_file_path


class ProcessFileDoFn(beam.DoFn):
    def process(self, element):
        file_path = element.path
        file_name = file_path.split("/")[-1]
        date_str = 'exec_date=' + file_name.split('_')[3][:8]
        new_file_path = f"{date_str}/{file_name}"
        processed_data = []
        with beam.io.gcsio.GcsIO().open(file_path, 'r') as f:
            # Read the lines from the file
            lines = f.read().decode('utf-8').split('\n')
            # Skip the first two rows
            lines = lines[2:]
            for line in lines:
                values = line.split(',')
                if len(values) >= 133:
                    logging.info('line>>>>>>>>>>>>>' + file_name)
                    logging.info('line>>>>>>>>>>>>>' + line)
                    logging.info('len>>>>>>>>>>>>>' + str(len(values)))
                    record = {
                        'cdrRecordType': str(values[0]),
                        'globalCallID_callManagerId': str(values[1]),
                        'globalCallID_callId': str(values[2]),
                        'globalCallId_ClusterID': str(values[67]),
                        'pkid': str(values[52]),
                    }
                    processed_data.append(record)
        return [(new_file_path, processed_data)]


class WriteToBigQueryDoFn(beam.DoFn):
    def __init__(self):
        self.client = None
        self.table_ref = None

    def start_bundle(self):
        from google.cloud import bigquery
        self.client = bigquery.Client(project=PROJECT_ID)
        self.table_ref = self.client.get_dataset(DATASET_ID).table(table_id=TABLE_ID)

    def process(self, element):
        file_path, processed_data = element
        if processed_data:
            # Upload the data to BigQuery
            table = self.client.get_table(self.table_ref)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.schema = schema  # Set the schema explicitly
            load_job = self.client.load_table_from_json(processed_data, table, job_config=job_config)
            load_job.result()  # Waits for the job to complete.
            if load_job.errors:
                logging.error(f"Failed to insert rows into BigQuery table {self.table_ref}: {load_job.errors}")
            else:
                logging.info(f"Inserted {len(processed_data)} rows into BigQuery table {self.table_ref}")
        return [file_path]


def write_to_output_file(file_path):
    output_bucket = "gs://df-data-output"
    output_path = output_bucket + '/' + file_path
    with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
        f.write('\n'.join(file_path[1]))


def run(argv=None):
    input_bucket = "gs://bks-pipeline-test"
    output_bucket = "gs://df-data-output"

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Create Input File Patterns" >> beam.Create([input_bucket + "/*"])

        input_files = (
                input_file_patterns
                | "Match All Input Files" >> fileio.MatchAll()
                | "Process Files" >> beam.ParDo(ProcessFileDoFn())
                | "Write To BigQuery And GCS" >> beam.ParDo(WriteToBigQueryDoFn()).with_outputs("gcs_output",
                                                                                                main="bq_output")
        )

        input_files.gcs_output | "Write Output Files" >> beam.Map(write_to_output_file)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
