import logging
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"


def process_file(file_path):
    # Process the file here, and return the processed data
    data = beam.io.ReadFromText(file_path.path)
    return data


def create_new_file_path(file_metadata):
    original_file_name = file_metadata.path.split("/")[-1]
    logging.info('original_file_name>>>>>>>>>>>>>' + original_file_name)
    date_str = 'exec_date=' + original_file_name.split('_')[3][:8]
    new_file_path = f"{date_str}/{original_file_name}"
    logging.info('new_file_path>>>>>>>>>>>>>' + new_file_path)
    return new_file_path


def print_file_content(file_path):
    with beam.io.gcsio.GcsIO().open(file_path.path, 'r') as f:
        file_contents = f.read()
    # file_contents = beam.io.ReadFromText(file_path.path)
    file_path = create_new_file_path(file_path)
    # print(f"File name: {file_path}\nContent: {file_contents}")
    return file_path, file_contents


def write_to_output_file(file_meta_data):
    file_path, file_contents = file_meta_data
    output_bucket = "gs://df-data-output"
    output_path = output_bucket + '/' + file_path
    with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
        f.write(file_contents)


def run(argv=None):
    input_bucket = "gs://bks-pipeline-test"
    output_bucket = "gs://df-data-output"

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).project = 'vivid-argon-376508'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"

    with beam.Pipeline(options=pipeline_options) as p:
        input_file_patterns = p | "Create Input File Patterns" >> beam.Create([input_bucket + "/*"])

        input_files = (
                input_file_patterns
                | "Match All Input Files" >> fileio.MatchAll()
                | "Print Input Files" >> beam.Map(print_file_content)
                | "Write Output Files" >> beam.Map(write_to_output_file)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
