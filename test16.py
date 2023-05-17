import logging
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"
PROJECT_ID = "vivid-argon-376508"


class ReadContent(beam.DoFn):
    def process(self, element):
        file_path = element.path
        gcs_client = beam.io.gcsio.GcsIO()
        with gcs_client.open(file_path) as f:
            content = f.read().decode('utf-8')
        lines = content.splitlines()
        filename = file_path.split('/')[-1]
        for line in lines:
            if 'cdrRecordType'.lower() not in line.lower() and 'INTEGER'.lower() not in line.lower():
                values = line.split(',')
                record = {
                    'filename': filename,
                    'cdrRecordType': str(values[0]),
                    'globalCallID_callManagerId': str(values[1]),
                    'globalCallID_callId': str(values[2]),
                    'globalCallId_ClusterID': str(values[67]),
                    'pkid': str(values[52]),
                }
                yield {'file_path': file_path, 'filename': filename, 'content': content, 'record': record}


class MoveFilesToNewGCSBucket(beam.DoFn):
    def process(self, element):
        filename = element['filename']
        file_path = element['file_path']
        content = element['content']
        output_bucket = "gs://df-data-output"
        date_str = 'exec_date=' + filename.split('_')[3][:8]
        new_file_path = f"{date_str}/{filename}"
        output_path = output_bucket + '/' + new_file_path
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            f.write(content.encode('utf-8'))
        beam.io.gcsio.GcsIO().delete(file_path)


def run(argv=None):
    # Hardcode your input parameters here
    input_path = "gs://bks-pipeline-test/cdr*"
    window_size = 2 * 60  # 2 minutes in seconds
    output_table = "vivid-argon-376508.bksdatasets001.dataflow-pipeline-test"
    destination_bucket = "gs://df-data-output/"

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Match All New Files" >> fileio.MatchContinuously(f"gs://bks-pipeline-test/*",
                                                                                    interval=60.0)
        files_content = (
                input_file_patterns
                | "Read Content" >> beam.ParDo(ReadContent())
        )

        _ = (
                files_content
                | "Extract Fields" >> beam.Map(lambda element: {
            'filename': element['filename'],
            'cdrRecordType': element['record']['cdrRecordType'],
            'globalCallID_callManagerId': element['record']['globalCallID_callManagerId'],
            'globalCallID_callId': element['record']['globalCallID_callId'],
            'globalCallId_ClusterID': element['record']['globalCallId_ClusterID'],
            'pkid': element['record']['pkid']
        })
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema="filename:STRING, cdrRecordType:STRING, globalCallID_callManagerId:STRING, globalCallID_callId:STRING, globalCallId_ClusterID:STRING, pkid:STRING",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        )

        _ = (
                files_content
                | "Write to GCS" >> beam.ParDo(MoveFilesToNewGCSBucket())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
