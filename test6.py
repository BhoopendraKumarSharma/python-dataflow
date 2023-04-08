import logging
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
import json
from google.cloud import bigquery
from apache_beam.io.gcp import gcsfilesystem

# Set environment variables
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
        with beam.io.gcsio.GcsIO().open(file_path, 'r') as f:
            # Read the lines from the file
            file_content = f.read().decode('utf-8')
            lines = file_content.split('\n')
            # Skip the first two rows
            lines = lines[2:]
            for line in lines:
                values = line.split(',')
                if len(values) >= 133:
                    logging.info('line>>>>>>>>>>>>>' + file_name)
                    logging.info('line>>>>>>>>>>>>>' + line)
                    logging.info('len>>>>>>>>>>>>>' + str(len(values)))
                    # Create a dictionary with the field names as keys and the values as values
                    record = {
                        'cdrRecordType': str(values[0]),
                        'globalCallID_callManagerId': str(values[1]),
                        'globalCallID_callId': str(values[2]),
                        'globalCallId_ClusterID': str(values[67]),
                        'pkid': str(values[52]),
                    }
                    processed_records.append(record)
        return [(orig_file_path, new_file_path, file_content, processed_records)]


class WriteToBigQueryDoFn(beam.DoFn):
    """
    A DoFn that writes data to BigQuery and GCS.

    Args:
        beam.DoFn: A subclass of the Apache Beam DoFn class.

    Returns:
        A list containing the file_path.
    """

    def __init__(self):
        """
        Initializes the BigQuery client and table reference.
        """
        self.client = None
        self.table_ref = None

    def start_bundle(self):
        """
        Initializes the BigQuery client and table reference.
        """
        from google.cloud import bigquery
        self.client = bigquery.Client(project=PROJECT_ID)
        self.table_ref = self.client.get_dataset(DATASET_ID).table(table_id=TABLE_ID)

    def process(self, element):
        """
        Writes data to BigQuery and GCS.

        Args:
            element: A tuple of the form (file_path, file_content, processed_data).

        Returns:
            A list containing the file_path.
        """
        orig_file_path, file_path, file_content, processed_data = element
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
                if file_content:
                    # Write data to GCS
                    output_bucket = "gs://df-data-output"
                    output_path = output_bucket + '/' + file_path
                    with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
                        f.write(file_content.encode('utf-8'))
                        # To delete the file from source location; it will fail if permission is not granted for delete
                        beam.io.gcsio.GcsIO().delete(orig_file_path)
        return [file_path]


def run(argv=None):
    """Runs an Apache Beam pipeline to process and load data from input files to BigQuery and GCS.

    Args:
        argv: A list of command-line arguments.
    """
    # Set input and output bucket paths
    input_bucket = "gs://bks-pipeline-test"
    output_bucket = "gs://df-data-output"

    # Set pipeline options
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    # Define pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Create a PCollection of input file patterns
        input_file_patterns = p | "Create Input File Patterns" >> beam.Create([input_bucket + "/*"])

        # Match all input files and process them
        input_files = (
                input_file_patterns
                | "Match All Input Files" >> fileio.MatchAll()
                | "Window Into Fixed Intervals" >> beam.WindowInto(
                            beam.window.FixedWindows(300),
                            trigger=beam.trigger.Repeatedly(beam.trigger.AfterProcessingTime(300)),
                            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
                | "Process Files" >> beam.ParDo(ProcessFileDoFn())
                | "Write To BigQuery And GCS" >> beam.ParDo(WriteToBigQueryDoFn()).with_outputs("gcs_output",
                                                                                                main="bq_output")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
