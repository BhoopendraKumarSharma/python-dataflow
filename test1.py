import logging
import os
from apache_beam.io.gcp import gcsfilesystem
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"


def move_files(output_path, pipeline_options, file_metadata):
    logging.info(f"metadata.path: {file_metadata.metadata.path}")
    original_file_name = file_metadata.metadata.path.split("/")[-1]
    logging.info('original_file_name>>>>>>>>>>>>>' + original_file_name)
    date_str = 'exec_date=' + original_file_name.split('_')[3][:8]
    new_file_path = f"{date_str}/{original_file_name}"
    logging.info('new_file_path>>>>>>>>>>>>>' + new_file_path)
    return new_file_path


def run(argv=None):
    # Hardcode your input parameters here
    input_path = "gs://bks-pipeline-test/*"
    output_path = "gs://df-data-output/"

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).project = 'vivid-argon-376508'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Create File Patterns" >> beam.Create([input_path])

        move_my_files = (
                input_file_patterns
                | "MatchAllFiles2" >> fileio.MatchAll()
                | "ReadMatches2" >> fileio.ReadMatches()
                | "Copy files" >> beam.Map(lambda file_info: move_files(output_path, pipeline_options, file_info))
        )

        processed_files = (
                input_file_patterns
                | "MatchAllFiles" >> fileio.MatchAll()
                | "ReadMatches" >> fileio.ReadMatches()
                | "Process Files" >> beam.Map(lambda file_info: file_info.read())
            # Add your data processing logic here
        )

        # Write output to GCS
        output_files = processed_files | "WriteToGCS" >> beam.io.WriteToText(
            move_my_files,
            file_name_suffix='',
            shard_name_template='',
            coder=beam.coders.BytesCoder(),
            num_shards=1
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
