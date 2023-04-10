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
TABLE_ID = "cisco-cdr-raw-data"
schema = [
    bigquery.SchemaField('cdrRecordType', 'INTEGER'), bigquery.SchemaField('globalCallID_callManagerId', 'INTEGER'),
    bigquery.SchemaField('globalCallID_callId', 'INTEGER'), bigquery.SchemaField('origLegCallIdentifier', 'INTEGER'),
    bigquery.SchemaField('dateTimeOrigination', 'INTEGER'), bigquery.SchemaField('origNodeId', 'INTEGER'),
    bigquery.SchemaField('origSpan', 'INTEGER'), bigquery.SchemaField('origIpAddr', 'INTEGER'),
    bigquery.SchemaField('callingPartyNumber', 'STRING'),
    bigquery.SchemaField('callingPartyUnicodeLoginUserID', 'STRING'),
    bigquery.SchemaField('origCause_location', 'INTEGER'), bigquery.SchemaField('origCause_value', 'INTEGER'),
    bigquery.SchemaField('origPrecedenceLevel', 'INTEGER'),
    bigquery.SchemaField('origMediaTransportAddress_IP', 'INTEGER'),
    bigquery.SchemaField('origMediaTransportAddress_Port', 'INTEGER'),
    bigquery.SchemaField('origMediaCap_payloadCapability', 'INTEGER'),
    bigquery.SchemaField('origMediaCap_maxFramesPerPacket', 'INTEGER'),
    bigquery.SchemaField('origMediaCap_g723BitRate', 'INTEGER'), bigquery.SchemaField('origVideoCap_Codec', 'INTEGER'),
    bigquery.SchemaField('origVideoCap_Bandwidth', 'INTEGER'),
    bigquery.SchemaField('origVideoCap_Resolution', 'INTEGER'),
    bigquery.SchemaField('origVideoTransportAddress_IP', 'INTEGER'),
    bigquery.SchemaField('origVideoTransportAddress_Port', 'INTEGER'),
    bigquery.SchemaField('origRSVPAudioStat', 'STRING'), bigquery.SchemaField('origRSVPVideoStat', 'STRING'),
    bigquery.SchemaField('destLegIdentifier', 'INTEGER'), bigquery.SchemaField('destNodeId', 'INTEGER'),
    bigquery.SchemaField('destSpan', 'INTEGER'), bigquery.SchemaField('destIpAddr', 'INTEGER'),
    bigquery.SchemaField('originalCalledPartyNumber', 'STRING'),
    bigquery.SchemaField('finalCalledPartyNumber', 'STRING'),
    bigquery.SchemaField('finalCalledPartyUnicodeLoginUserID', 'STRING'),
    bigquery.SchemaField('destCause_location', 'INTEGER'), bigquery.SchemaField('destCause_value', 'INTEGER'),
    bigquery.SchemaField('destPrecedenceLevel', 'INTEGER'),
    bigquery.SchemaField('destMediaTransportAddress_IP', 'INTEGER'),
    bigquery.SchemaField('destMediaTransportAddress_Port', 'INTEGER'),
    bigquery.SchemaField('destMediaCap_payloadCapability', 'INTEGER'),
    bigquery.SchemaField('destMediaCap_maxFramesPerPacket', 'INTEGER'),
    bigquery.SchemaField('destMediaCap_g723BitRate', 'INTEGER'), bigquery.SchemaField('destVideoCap_Codec', 'INTEGER'),
    bigquery.SchemaField('destVideoCap_Bandwidth', 'INTEGER'),
    bigquery.SchemaField('destVideoCap_Resolution', 'INTEGER'),
    bigquery.SchemaField('destVideoTransportAddress_IP', 'INTEGER'),
    bigquery.SchemaField('destVideoTransportAddress_Port', 'INTEGER'),
    bigquery.SchemaField('destRSVPAudioStat', 'STRING'), bigquery.SchemaField('destRSVPVideoStat', 'STRING'),
    bigquery.SchemaField('dateTimeConnect', 'INTEGER'), bigquery.SchemaField('dateTimeDisconnect', 'INTEGER'),
    bigquery.SchemaField('lastRedirectDn', 'STRING'), bigquery.SchemaField('pkid', 'STRING'),
    bigquery.SchemaField('originalCalledPartyNumberPartition', 'STRING'),
    bigquery.SchemaField('callingPartyNumberPartition', 'STRING'),
    bigquery.SchemaField('finalCalledPartyNumberPartition', 'STRING'),
    bigquery.SchemaField('lastRedirectDnPartition', 'STRING'), bigquery.SchemaField('duration', 'INTEGER'),
    bigquery.SchemaField('origDeviceName', 'STRING'), bigquery.SchemaField('destDeviceName', 'STRING'),
    bigquery.SchemaField('origCallTerminationOnBehalfOf', 'INTEGER'),
    bigquery.SchemaField('destCallTerminationOnBehalfOf', 'INTEGER'),
    bigquery.SchemaField('origCalledPartyRedirectOnBehalfOf', 'INTEGER'),
    bigquery.SchemaField('lastRedirectRedirectOnBehalfOf', 'INTEGER'),
    bigquery.SchemaField('origCalledPartyRedirectReason', 'INTEGER'),
    bigquery.SchemaField('lastRedirectRedirectReason', 'INTEGER'),
    bigquery.SchemaField('destConversationId', 'INTEGER'), bigquery.SchemaField('globalCallId_ClusterID', 'STRING'),
    bigquery.SchemaField('joinOnBehalfOf', 'INTEGER'), bigquery.SchemaField('comment', 'STRING'),
    bigquery.SchemaField('authCodeDescription', 'STRING'), bigquery.SchemaField('authorizationLevel', 'INTEGER'),
    bigquery.SchemaField('clientMatterCode', 'STRING'), bigquery.SchemaField('origDTMFMethod', 'INTEGER'),
    bigquery.SchemaField('destDTMFMethod', 'INTEGER'), bigquery.SchemaField('callSecuredStatus', 'INTEGER'),
    bigquery.SchemaField('origConversationId', 'INTEGER'), bigquery.SchemaField('origMediaCap_Bandwidth', 'INTEGER'),
    bigquery.SchemaField('destMediaCap_Bandwidth', 'INTEGER'), bigquery.SchemaField('authorizationCodeValue', 'STRING'),
    bigquery.SchemaField('outpulsedCallingPartyNumber', 'STRING'),
    bigquery.SchemaField('outpulsedCalledPartyNumber', 'STRING'), bigquery.SchemaField('origIpv4v6Addr', 'STRING'),
    bigquery.SchemaField('destIpv4v6Addr', 'STRING'), bigquery.SchemaField('origVideoCap_Codec_Channel2', 'INTEGER'),
    bigquery.SchemaField('origVideoCap_Bandwidth_Channel2', 'INTEGER'),
    bigquery.SchemaField('origVideoCap_Resolution_Channel2', 'INTEGER'),
    bigquery.SchemaField('origVideoTransportAddress_IP_Channel2', 'INTEGER'),
    bigquery.SchemaField('origVideoTransportAddress_Port_Channel2', 'INTEGER'),
    bigquery.SchemaField('origVideoChannel_Role_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoCap_Codec_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoCap_Bandwidth_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoCap_Resolution_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoTransportAddress_IP_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoTransportAddress_Port_Channel2', 'INTEGER'),
    bigquery.SchemaField('destVideoChannel_Role_Channel2', 'INTEGER'),
    bigquery.SchemaField('IncomingProtocolID', 'INTEGER'), bigquery.SchemaField('IncomingProtocolCallRef', 'STRING'),
    bigquery.SchemaField('OutgoingProtocolID', 'INTEGER'), bigquery.SchemaField('OutgoingProtocolCallRef', 'STRING'),
    bigquery.SchemaField('currentRoutingReason', 'INTEGER'), bigquery.SchemaField('origRoutingReason', 'INTEGER'),
    bigquery.SchemaField('lastRedirectingRoutingReason', 'INTEGER'),
    bigquery.SchemaField('huntPilotPartition', 'STRING'), bigquery.SchemaField('huntPilotDN', 'STRING'),
    bigquery.SchemaField('calledPartyPatternUsage', 'INTEGER'), bigquery.SchemaField('IncomingICID', 'STRING'),
    bigquery.SchemaField('IncomingOrigIOI', 'STRING'), bigquery.SchemaField('IncomingTermIOI', 'STRING'),
    bigquery.SchemaField('OutgoingICID', 'STRING'), bigquery.SchemaField('OutgoingOrigIOI', 'STRING'),
    bigquery.SchemaField('OutgoingTermIOI', 'STRING'),
    bigquery.SchemaField('outpulsedOriginalCalledPartyNumber', 'STRING'),
    bigquery.SchemaField('outpulsedLastRedirectingNumber', 'STRING'), bigquery.SchemaField('wasCallQueued', 'INTEGER'),
    bigquery.SchemaField('totalWaitTimeInQueue', 'INTEGER'), bigquery.SchemaField('callingPartyNumber_uri', 'STRING'),
    bigquery.SchemaField('originalCalledPartyNumber_uri', 'STRING'),
    bigquery.SchemaField('finalCalledPartyNumber_uri', 'STRING'), bigquery.SchemaField('lastRedirectDn_uri', 'STRING'),
    bigquery.SchemaField('mobileCallingPartyNumber', 'STRING'),
    bigquery.SchemaField('finalMobileCalledPartyNumber', 'STRING'),
    bigquery.SchemaField('origMobileDeviceName', 'STRING'), bigquery.SchemaField('destMobileDeviceName', 'STRING'),
    bigquery.SchemaField('origMobileCallDuration', 'INTEGER'),
    bigquery.SchemaField('destMobileCallDuration', 'INTEGER'), bigquery.SchemaField('mobileCallType', 'INTEGER'),
    bigquery.SchemaField('originalCalledPartyPattern', 'STRING'),
    bigquery.SchemaField('finalCalledPartyPattern', 'STRING'),
    bigquery.SchemaField('lastRedirectingPartyPattern', 'STRING'), bigquery.SchemaField('huntPilotPattern', 'STRING'),
    bigquery.SchemaField('origDeviceType', 'STRING'), bigquery.SchemaField('destDeviceType', 'STRING'),
    bigquery.SchemaField('origDeviceSessionID', 'STRING'), bigquery.SchemaField('destDeviceSessionID', 'STRING'), bigquery.SchemaField('filename', 'STRING'),
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
            header_line = lines[0]
            header_fields = header_line.split(',')
            header_fields = [field.strip('"') for field in header_fields]
            lines = lines[2:]
            for line in lines:
                values = line.split(',')
                if len(values) >= len(header_fields):
                    logging.info('line>>>>>>>>>>>>>' + file_name)
                    logging.info('line>>>>>>>>>>>>>' + line)
                    logging.info('len>>>>>>>>>>>>>' + str(len(values)))
                    # Create a dictionary with the field names as keys and the values as values
                    record = {}
                    for i in range(len(header_fields)):
                        field_value = values[i].strip('"')
                        if field_value != "":
                            record[header_fields[i]] = field_value
                        else:
                            record[header_fields[i]] = None
                    record['filename'] = file_name
                    processed_records.append(record)
            logging.info(processed_records)
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
        input_file_patterns = p | "Match All New Files" >> fileio.MatchContinuously(f"gs://bks-pipeline-test/*",
                                                                                    interval=60.0)
        input_files = (
                input_file_patterns
                | "Process Files" >> beam.ParDo(ProcessFileDoFn())
                | "Write To BigQuery And GCS" >> beam.ParDo(WriteToBigQueryDoFn()).with_outputs("gcs_output",
                                                                                                main="bq_output")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
