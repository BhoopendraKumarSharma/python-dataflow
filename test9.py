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
schema = {'fields': [
    {'name': 'cdrRecordType', 'type': 'INTEGER'}, {'name': 'globalCallID_callManagerId', 'type': 'INTEGER'},
    {'name': 'globalCallID_callId', 'type': 'INTEGER'}, {'name': 'origLegCallIdentifier', 'type': 'INTEGER'},
    {'name': 'dateTimeOrigination', 'type': 'INTEGER'}, {'name': 'origNodeId', 'type': 'INTEGER'},
    {'name': 'origSpan', 'type': 'INTEGER'}, {'name': 'origIpAddr', 'type': 'INTEGER'},
    {'name': 'callingPartyNumber', 'type': 'STRING'},
    {'name': 'callingPartyUnicodeLoginUserID', 'type': 'STRING'},
    {'name': 'origCause_location', 'type': 'INTEGER'}, {'name': 'origCause_value', 'type': 'INTEGER'},
    {'name': 'origPrecedenceLevel', 'type': 'INTEGER'},
    {'name': 'origMediaTransportAddress_IP', 'type': 'INTEGER'},
    {'name': 'origMediaTransportAddress_Port', 'type': 'INTEGER'},
    {'name': 'origMediaCap_payloadCapability', 'type': 'INTEGER'},
    {'name': 'origMediaCap_maxFramesPerPacket', 'type': 'INTEGER'},
    {'name': 'origMediaCap_g723BitRate', 'type': 'INTEGER'}, {'name': 'origVideoCap_Codec', 'type': 'INTEGER'},
    {'name': 'origVideoCap_Bandwidth', 'type': 'INTEGER'},
    {'name': 'origVideoCap_Resolution', 'type': 'INTEGER'},
    {'name': 'origVideoTransportAddress_IP', 'type': 'INTEGER'},
    {'name': 'origVideoTransportAddress_Port', 'type': 'INTEGER'},
    {'name': 'origRSVPAudioStat', 'type': 'STRING'}, {'name': 'origRSVPVideoStat', 'type': 'STRING'},
    {'name': 'destLegIdentifier', 'type': 'INTEGER'}, {'name': 'destNodeId', 'type': 'INTEGER'},
    {'name': 'destSpan', 'type': 'INTEGER'}, {'name': 'destIpAddr', 'type': 'INTEGER'},
    {'name': 'originalCalledPartyNumber', 'type': 'STRING'},
    {'name': 'finalCalledPartyNumber', 'type': 'STRING'},
    {'name': 'finalCalledPartyUnicodeLoginUserID', 'type': 'STRING'},
    {'name': 'destCause_location', 'type': 'INTEGER'}, {'name': 'destCause_value', 'type': 'INTEGER'},
    {'name': 'destPrecedenceLevel', 'type': 'INTEGER'},
    {'name': 'destMediaTransportAddress_IP', 'type': 'INTEGER'},
    {'name': 'destMediaTransportAddress_Port', 'type': 'INTEGER'},
    {'name': 'destMediaCap_payloadCapability', 'type': 'INTEGER'},
    {'name': 'destMediaCap_maxFramesPerPacket', 'type': 'INTEGER'},
    {'name': 'destMediaCap_g723BitRate', 'type': 'INTEGER'}, {'name': 'destVideoCap_Codec', 'type': 'INTEGER'},
    {'name': 'destVideoCap_Bandwidth', 'type': 'INTEGER'},
    {'name': 'destVideoCap_Resolution', 'type': 'INTEGER'},
    {'name': 'destVideoTransportAddress_IP', 'type': 'INTEGER'},
    {'name': 'destVideoTransportAddress_Port', 'type': 'INTEGER'},
    {'name': 'destRSVPAudioStat', 'type': 'STRING'}, {'name': 'destRSVPVideoStat', 'type': 'STRING'},
    {'name': 'dateTimeConnect', 'type': 'INTEGER'}, {'name': 'dateTimeDisconnect', 'type': 'INTEGER'},
    {'name': 'lastRedirectDn', 'type': 'STRING'}, {'name': 'pkid', 'type': 'STRING'},
    {'name': 'originalCalledPartyNumberPartition', 'type': 'STRING'},
    {'name': 'callingPartyNumberPartition', 'type': 'STRING'},
    {'name': 'finalCalledPartyNumberPartition', 'type': 'STRING'},
    {'name': 'lastRedirectDnPartition', 'type': 'STRING'}, {'name': 'duration', 'type': 'INTEGER'},
    {'name': 'origDeviceName', 'type': 'STRING'}, {'name': 'destDeviceName', 'type': 'STRING'},
    {'name': 'origCallTerminationOnBehalfOf', 'type': 'INTEGER'},
    {'name': 'destCallTerminationOnBehalfOf', 'type': 'INTEGER'},
    {'name': 'origCalledPartyRedirectOnBehalfOf', 'type': 'INTEGER'},
    {'name': 'lastRedirectRedirectOnBehalfOf', 'type': 'INTEGER'},
    {'name': 'origCalledPartyRedirectReason', 'type': 'INTEGER'},
    {'name': 'lastRedirectRedirectReason', 'type': 'INTEGER'},
    {'name': 'destConversationId', 'type': 'INTEGER'}, {'name': 'globalCallId_ClusterID', 'type': 'STRING'},
    {'name': 'joinOnBehalfOf', 'type': 'INTEGER'}, {'name': 'comment', 'type': 'STRING'},
    {'name': 'authCodeDescription', 'type': 'STRING'}, {'name': 'authorizationLevel', 'type': 'INTEGER'},
    {'name': 'clientMatterCode', 'type': 'STRING'}, {'name': 'origDTMFMethod', 'type': 'INTEGER'},
    {'name': 'destDTMFMethod', 'type': 'INTEGER'}, {'name': 'callSecuredStatus', 'type': 'INTEGER'},
    {'name': 'origConversationId', 'type': 'INTEGER'}, {'name': 'origMediaCap_Bandwidth', 'type': 'INTEGER'},
    {'name': 'destMediaCap_Bandwidth', 'type': 'INTEGER'}, {'name': 'authorizationCodeValue', 'type': 'STRING'},
    {'name': 'outpulsedCallingPartyNumber', 'type': 'STRING'},
    {'name': 'outpulsedCalledPartyNumber', 'type': 'STRING'}, {'name': 'origIpv4v6Addr', 'type': 'STRING'},
    {'name': 'destIpv4v6Addr', 'type': 'STRING'}, {'name': 'origVideoCap_Codec_Channel2', 'type': 'INTEGER'},
    {'name': 'origVideoCap_Bandwidth_Channel2', 'type': 'INTEGER'},
    {'name': 'origVideoCap_Resolution_Channel2', 'type': 'INTEGER'},
    {'name': 'origVideoTransportAddress_IP_Channel2', 'type': 'INTEGER'},
    {'name': 'origVideoTransportAddress_Port_Channel2', 'type': 'INTEGER'},
    {'name': 'origVideoChannel_Role_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoCap_Codec_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoCap_Bandwidth_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoCap_Resolution_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoTransportAddress_IP_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoTransportAddress_Port_Channel2', 'type': 'INTEGER'},
    {'name': 'destVideoChannel_Role_Channel2', 'type': 'INTEGER'},
    {'name': 'IncomingProtocolID', 'type': 'INTEGER'}, {'name': 'IncomingProtocolCallRef', 'type': 'STRING'},
    {'name': 'OutgoingProtocolID', 'type': 'INTEGER'}, {'name': 'OutgoingProtocolCallRef', 'type': 'STRING'},
    {'name': 'currentRoutingReason', 'type': 'INTEGER'}, {'name': 'origRoutingReason', 'type': 'INTEGER'},
    {'name': 'lastRedirectingRoutingReason', 'type': 'INTEGER'},
    {'name': 'huntPilotPartition', 'type': 'STRING'}, {'name': 'huntPilotDN', 'type': 'STRING'},
    {'name': 'calledPartyPatternUsage', 'type': 'INTEGER'}, {'name': 'IncomingICID', 'type': 'STRING'},
    {'name': 'IncomingOrigIOI', 'type': 'STRING'}, {'name': 'IncomingTermIOI', 'type': 'STRING'},
    {'name': 'OutgoingICID', 'type': 'STRING'}, {'name': 'OutgoingOrigIOI', 'type': 'STRING'},
    {'name': 'OutgoingTermIOI', 'type': 'STRING'},
    {'name': 'outpulsedOriginalCalledPartyNumber', 'type': 'STRING'},
    {'name': 'outpulsedLastRedirectingNumber', 'type': 'STRING'}, {'name': 'wasCallQueued', 'type': 'INTEGER'},
    {'name': 'totalWaitTimeInQueue', 'type': 'INTEGER'}, {'name': 'callingPartyNumber_uri', 'type': 'STRING'},
    {'name': 'originalCalledPartyNumber_uri', 'type': 'STRING'},
    {'name': 'finalCalledPartyNumber_uri', 'type': 'STRING'}, {'name': 'lastRedirectDn_uri', 'type': 'STRING'},
    {'name': 'mobileCallingPartyNumber', 'type': 'STRING'},
    {'name': 'finalMobileCalledPartyNumber', 'type': 'STRING'},
    {'name': 'origMobileDeviceName', 'type': 'STRING'}, {'name': 'destMobileDeviceName', 'type': 'STRING'},
    {'name': 'origMobileCallDuration', 'type': 'INTEGER'},
    {'name': 'destMobileCallDuration', 'type': 'INTEGER'}, {'name': 'mobileCallType', 'type': 'INTEGER'},
    {'name': 'originalCalledPartyPattern', 'type': 'STRING'},
    {'name': 'finalCalledPartyPattern', 'type': 'STRING'},
    {'name': 'lastRedirectingPartyPattern', 'type': 'STRING'}, {'name': 'huntPilotPattern', 'type': 'STRING'},
    {'name': 'origDeviceType', 'type': 'STRING'}, {'name': 'destDeviceType', 'type': 'STRING'},
    {'name': 'origDeviceSessionID', 'type': 'STRING'}, {'name': 'destDeviceSessionID', 'type': 'STRING'},
    {'name': 'filename', 'type': 'STRING'}
]}


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
                    # logging.info('line>>>>>>>>>>>>>' + file_name)
                    # logging.info('line>>>>>>>>>>>>>' + line)
                    # logging.info('len>>>>>>>>>>>>>' + str(len(values)))
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
            logging.info('processed_data>>>>>>>>>>>>>')
            logging.info(processed_records)
        return [(orig_file_path, new_file_path, file_content, processed_records)]


class WriteToBigQueryAndGCSDoFn(beam.DoFn):
    def process(self, element):
        orig_file_path, file_path, file_content, processed_data = element
        if processed_data:
            if file_content:
                # Write data to GCS
                output_bucket = "gs://df-data-output"
                output_path = output_bucket + '/' + file_path
                with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
                    f.write(file_content.encode('utf-8'))
                    # To delete the file from source location; it will fail if permission is not granted for delete
                    beam.io.gcsio.GcsIO().delete(orig_file_path)
            return processed_data


def parse_csv(line):
    # skip header row
    if 'cdrRecordType' not in line:
        values = line.split(',')
        record = {
            'cdrRecordType': str(values[0]),
            'globalCallID_callManagerId': str(values[1]),
            'globalCallID_callId': str(values[2]),
            'globalCallId_ClusterID': str(values[67]),
            'pkid': str(values[52]),
        }
        return record


def run(argv=None):
    """Runs an Apache Beam pipeline to process and load data from input files to BigQuery and GCS.

    Args:
        argv: A list of command-line arguments.
    """
    # Set input and output bucket paths
    input_bucket = "gs://bks-pipeline-test"
    output_bucket = "gs://df-data-output"
    output_table = "vivid-argon-376508:bksdatasets001.dataflow-pipeline-test"

    # Set pipeline options
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    # Define pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        logging.info('Pipeline started')
        input_file_patterns = p | "Match All New Files" >> fileio.MatchContinuously(f"gs://bks-pipeline-test/*",
                                                                                    interval=60.0)

        csv_lines = (
                input_file_patterns
                | "ReadAll from GCS" >> beam.io.ReadAllFromText(skip_header_lines=2)
                | "ParseCSV" >> beam.Map(parse_csv)
        )

        bq_output = (
                csv_lines
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema='cdrRecordType:STRING,globalCallID_callManagerId:STRING,globalCallID_callId:STRING,globalCallId_ClusterID:STRING,pkid:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        )

        input_files = (
                input_file_patterns
                | "Process Files" >> beam.ParDo(ProcessFileDoFn())
                | "Prepare data" >> beam.ParDo(WriteToBigQueryAndGCSDoFn())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
