import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io import WriteToText
from datetime import datetime

# Define your input and output paths
input_path = "gs://my-bucket/input/*.txt"
output_table = "my-project:my_dataset.my_table"
output_path = "gs://my-bucket/output"

# Define your Dataflow options
options = PipelineOptions()
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).project = "my-project"
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).job_name = "my-job"
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).staging_location = "gs://my-bucket/staging"
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).temp_location = "gs://my-bucket/temp"
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).subnetwork = StaticValueProvider(str,
                                                                                       'projects/my-project/regions'
                                                                                       '/us-central1/subnetworks/my'
                                                                                       '-subnetwork')
options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions).use_public_ips = False  # Set to True for public IPs, False for
# private IPs


# Define your processing function
def process_file(line):
    location = line.strip()
    # Perform any necessary data transformations here
    return {"location": location}


# Define your Dataflow pipeline
with beam.Pipeline(options=options) as p:
    # Read text files from GCS
    lines = p | "Read from GCS" >> beam.io.ReadFromText(input_path)

    # Process the data using the transform function
    transformed = lines | "Transform Data" >> beam.Map(process_file)

    # Write the data to BigQuery
    transformed | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        output_table,
        schema="location:STRING",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    # Move processed files to a new bucket
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_prefix = f"{output_path}/processed_files_{timestamp}"
    lines | "Write to GCS" >> WriteToText(output_prefix)

    # Delete original files from input bucket
    p | "Delete input files" >> beam.io.gcp.gcs.DeleteFromGCS(input_path)
