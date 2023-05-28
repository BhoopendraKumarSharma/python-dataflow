import logging
import typing
import os
import datetime, time
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Hardcoded input parameters
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"
PROJECT_ID = "vivid-argon-376508"
input_path = "gs://bks-pipeline-test/*"
output_table = "vivid-argon-376508.bksdatasets001.cisco-cdr-raw-data"
output_success_bucket = "gs://df-data-output"
output_failure_bucket = "gs://df-data-output-failed-files"


class ReadContent(beam.DoFn):
    """
    A DoFn class that reads content from a given file path, processes it, and outputs a dictionary record of the data.
    Also provides error handling to catch exceptions during the file processing.
    """

    # Define tags for the file content output and error output
    file_content_output_tag = 'file-content-output'

    def process(self, element):
        """
        The process method is automatically called by the Beam SDK to process an element in the input PCollection.

        Args:
            element: An element from the input PCollection, expected to be a file path.

        Yields: TaggedOutput: A dictionary record of the file data if processing is successful, or an error record if
        an exception is encountered.
        """
        try:
            time.sleep(5)  # Wait for 5 seconds before starting processing
            file_path = element.path
            # Extracting the filename from the file path
            file_name = file_path.split("/")[-1]
            # Extracting the date from the filename
            file_date = file_name.split('_')[3][:8]
            # Formatting the date string
            date_str = 'exec_date=' + file_date[:4] + '-' + file_date[4:6] + '-' + file_date[6:8]
            # Constructing the new file path
            new_file_path = f"{date_str}/{file_name}"
            logging.info(f"Processing file: {file_name}")
            content = None

            # Open and read the file
            try:
                with beam.io.gcsio.GcsIO().open(file_path, 'r') as f:
                    content = f.read().decode('utf-8')
            except IOError:
                logging.info(f"File {file_name} is already processed")
                yield {'orig_file_path': file_path, 'new_file_path': new_file_path,
                       'content': None,
                       'record': None, 'status': 2}

            # Splitting the content into lines
            lines = content.split('\n')

            # Extracting the header fields
            header_line = lines[0]
            header_fields = header_line.split(',')
            header_fields = [field.strip('"') for field in header_fields]

            # Skipping the first two lines (header and empty line)
            lines = lines[2:]

            # Processing each line in the file
            for line in lines:
                # If the line is not empty
                if line.strip() != "":
                    values = line.split(',')

                    # Creating a dictionary record for the line
                    record = {}
                    for i in range(len(header_fields)):
                        # If the current field has a corresponding value in the line
                        if i < len(values):
                            field_value = values[i].strip('"')
                            if field_value != "":
                                record[header_fields[i]] = field_value
                            else:
                                record[header_fields[i]] = None
                        # If the current field does not have a corresponding value in the line
                        else:
                            record[header_fields[i]] = None

                    # Adding additional fields to the record
                    record['filename'] = file_name
                    extract_storeID = lambda s: s.split('_')[1] if s and '_' in s else None
                    record['storeID'] = extract_storeID(record.get('originalCalledPartyNumberPartition', None))
                    timestamp = time.time()
                    record['processing_timestamp'] = datetime.datetime.fromtimestamp(timestamp).isoformat()
                    record['file_date'] = datetime.datetime.strptime(file_date, "%Y%m%d").date()
                    record['dateTimeOrigination'] = datetime.datetime.fromtimestamp(
                        int(record['dateTimeOrigination'])).isoformat() if record['dateTimeOrigination'] not in [None,
                                                                                                                 "", 0,
                                                                                                                 "0"] else None
                    record['dateTimeConnect'] = datetime.datetime.fromtimestamp(
                        int(record['dateTimeConnect'])).isoformat() if record[
                                                                           'dateTimeConnect'] not in [None, "", 0,
                                                                                                      "0"] else None
                    record['dateTimeDisconnect'] = datetime.datetime.fromtimestamp(
                        int(record['dateTimeDisconnect'])).isoformat() if record[
                                                                              'dateTimeDisconnect'] not in [None, "", 0,
                                                                                                            "0"] else None
                    # Yielding the processed record
                    # logging.info(record)
                    yield {'orig_file_path': file_path, 'new_file_path': new_file_path,
                           'record': record, 'status': 1}
            yield beam.pvalue.TaggedOutput(self.file_content_output_tag,
                                           {'orig_file_path': file_path, 'new_file_path': new_file_path,
                                            'content': content, 'status': 1})
        except Exception as e:
            # Logging the error and yielding an error record
            logging.error(f"Error processing file {file_path}: {str(e)}")
            yield beam.pvalue.TaggedOutput(self.file_content_output_tag,
                                           {'orig_file_path': file_path, 'new_file_path': new_file_path,
                                            'content': content, 'status': 0})


class MoveFilesToNewGCSBucket(beam.DoFn):
    """
    A DoFn class that moves files to a new GCS bucket. Files are written to different buckets based on their status.
    """

    def process(self, element):
        """
        The process method is automatically called by the Beam SDK to process an element in the input PCollection.

        Args: element: An element from the input PCollection, expected to be a dictionary with 'orig_file_path',
        'new_file_path', 'content' and 'status' keys.

        Yields: Nothing. But during the process, files are written to their respective GCS buckets and then deleted
        from the original location.
        """

        # Extracting necessary details from the element
        orig_file_path = element['orig_file_path']
        new_file_path = element['new_file_path']
        content = element['content']
        status = element['status']

        # Choosing the output bucket based on the 'status' of the file
        output_bucket = output_success_bucket if status else output_failure_bucket
        output_path = output_bucket + '/' + new_file_path

        try:
            # Writing the file content to the new location in the selected bucket
            if content:
                with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
                    f.write(content.encode('utf-8'))

                # Deleting the file from the original location. Note that this will fail if the necessary delete
                # permissions are not granted.
                beam.io.gcsio.GcsIO().delete(orig_file_path)
        except Exception as e:
            # Logging the error message along with the relevant file details
            logging.error(f"Error processing file {orig_file_path} with status {status}: {str(e)}")


def run(argv=None):
    """
    Run Apache Beam pipeline for processing files from Google Cloud Storage (GCS)
    and writing the content to BigQuery and another GCS bucket.

    Parameters:
    argv (list): list of arguments

    """
    # Define pipeline options
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-central1"
    pipeline_options.view_as(StandardOptions).streaming = True

    # Create a pipeline with the defined options
    with beam.Pipeline(options=pipeline_options) as p:
        # Continuously match all new files in the given GCS bucket
        input_file_patterns = p | "Match All New Files" >> fileio.MatchContinuously(f"{input_path}",
                                                                                    interval=60.0)

        # Read file content and process it for data consumption and re-writing
        files_content_results = (
                input_file_patterns
                | "Read Content" >> beam.ParDo(ReadContent()).with_outputs(
            ReadContent.file_content_output_tag, main='main')
        )

        files_content_results_bq = files_content_results.main
        files_content_results_gcs = files_content_results[ReadContent.file_content_output_tag]

        # Filter out rows from already processed files
        new_files_content_results = (
                files_content_results_bq
                | "Filter Already Processed Files" >> beam.Filter(lambda element: element['status'] != 2)

        )

        # Filter out data for BQ inserts
        successful_files_content = (
                new_files_content_results
                | "Filter Successful Files" >> beam.Filter(lambda element: element['status'] == 1)
        )

        # Write the successfully processed files content to BigQuery

        write_to_bq = (
                successful_files_content
                | "Filter Non-Empty Records" >> beam.Filter(lambda element: element['record'] is not None)
                | "Extract Fields" >> beam.Map(lambda element: element['record'])
                | "Batch Elements" >> beam.BatchElements(min_batch_size=100,
                                                         max_batch_size=200)  # adjust sizes as needed
                | "Flatten Lists" >> beam.FlatMap(lambda batch: batch)
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=output_table,
            schema="cdrRecordType:INTEGER, globalCallID_callManagerId:INTEGER, globalCallID_callId:INTEGER, origLegCallIdentifier:INTEGER, dateTimeOrigination:TIMESTAMP, origNodeId:INTEGER, origSpan:INTEGER, origIpAddr:INTEGER, callingPartyNumber:STRING, callingPartyUnicodeLoginUserID:STRING, origCause_location:INTEGER, origCause_value:INTEGER, origPrecedenceLevel:INTEGER, origMediaTransportAddress_IP:INTEGER, origMediaTransportAddress_Port:INTEGER, origMediaCap_payloadCapability:INTEGER, origMediaCap_maxFramesPerPacket:INTEGER, origMediaCap_g723BitRate:INTEGER, origVideoCap_Codec:INTEGER, origVideoCap_Bandwidth:INTEGER, origVideoCap_Resolution:INTEGER, origVideoTransportAddress_IP:INTEGER, origVideoTransportAddress_Port:INTEGER, origRSVPAudioStat:STRING, origRSVPVideoStat:STRING, destLegIdentifier:INTEGER, destNodeId:INTEGER, destSpan:INTEGER, destIpAddr:INTEGER, originalCalledPartyNumber:STRING, finalCalledPartyNumber:STRING, finalCalledPartyUnicodeLoginUserID:STRING, destCause_location:INTEGER, destCause_value:INTEGER, destPrecedenceLevel:INTEGER, destMediaTransportAddress_IP:INTEGER, destMediaTransportAddress_Port:INTEGER, destMediaCap_payloadCapability:INTEGER, destMediaCap_maxFramesPerPacket:INTEGER, destMediaCap_g723BitRate:INTEGER, destVideoCap_Codec:INTEGER, destVideoCap_Bandwidth:INTEGER, destVideoCap_Resolution:INTEGER, destVideoTransportAddress_IP:INTEGER, destVideoTransportAddress_Port:INTEGER, destRSVPAudioStat:STRING, destRSVPVideoStat:STRING, dateTimeConnect:TIMESTAMP, dateTimeDisconnect:TIMESTAMP, lastRedirectDn:STRING, pkid:STRING, originalCalledPartyNumberPartition:STRING, callingPartyNumberPartition:STRING, finalCalledPartyNumberPartition:STRING, lastRedirectDnPartition:STRING, duration:INTEGER, origDeviceName:STRING, destDeviceName:STRING, origCallTerminationOnBehalfOf:INTEGER, destCallTerminationOnBehalfOf:INTEGER, origCalledPartyRedirectOnBehalfOf:INTEGER, lastRedirectRedirectOnBehalfOf:INTEGER, origCalledPartyRedirectReason:INTEGER, lastRedirectRedirectReason:INTEGER, destConversationId:INTEGER, globalCallId_ClusterID:STRING, joinOnBehalfOf:INTEGER, comment:STRING, authCodeDescription:STRING, authorizationLevel:INTEGER, clientMatterCode:STRING, origDTMFMethod:INTEGER, destDTMFMethod:INTEGER, callSecuredStatus:INTEGER, origConversationId:INTEGER, origMediaCap_Bandwidth:INTEGER, destMediaCap_Bandwidth:INTEGER, authorizationCodeValue:STRING, outpulsedCallingPartyNumber:STRING, outpulsedCalledPartyNumber:STRING, origIpv4v6Addr:STRING, destIpv4v6Addr:STRING, origVideoCap_Codec_Channel2:INTEGER, origVideoCap_Bandwidth_Channel2:INTEGER, origVideoCap_Resolution_Channel2:INTEGER, origVideoTransportAddress_IP_Channel2:INTEGER, origVideoTransportAddress_Port_Channel2:INTEGER, origVideoChannel_Role_Channel2:INTEGER, destVideoCap_Codec_Channel2:INTEGER, destVideoCap_Bandwidth_Channel2:INTEGER, destVideoCap_Resolution_Channel2:INTEGER, destVideoTransportAddress_IP_Channel2:INTEGER, destVideoTransportAddress_Port_Channel2:INTEGER, destVideoChannel_Role_Channel2:INTEGER, IncomingProtocolID:INTEGER, IncomingProtocolCallRef:STRING, OutgoingProtocolID:INTEGER, OutgoingProtocolCallRef:STRING, currentRoutingReason:INTEGER, origRoutingReason:INTEGER, lastRedirectingRoutingReason:INTEGER, huntPilotPartition:STRING, huntPilotDN:STRING, calledPartyPatternUsage:INTEGER, IncomingICID:STRING, IncomingOrigIOI:STRING, IncomingTermIOI:STRING, OutgoingICID:STRING, OutgoingOrigIOI:STRING, OutgoingTermIOI:STRING, outpulsedOriginalCalledPartyNumber:STRING, outpulsedLastRedirectingNumber:STRING, wasCallQueued:INTEGER, totalWaitTimeInQueue:INTEGER, callingPartyNumber_uri:STRING, originalCalledPartyNumber_uri:STRING, finalCalledPartyNumber_uri:STRING, lastRedirectDn_uri:STRING, mobileCallingPartyNumber:STRING, finalMobileCalledPartyNumber:STRING, origMobileDeviceName:STRING, destMobileDeviceName:STRING, origMobileCallDuration:INTEGER, destMobileCallDuration:INTEGER, mobileCallType:INTEGER, originalCalledPartyPattern:STRING, finalCalledPartyPattern:STRING, lastRedirectingPartyPattern:STRING, huntPilotPattern:STRING, origDeviceType:STRING, destDeviceType:STRING, origDeviceSessionID:STRING, destDeviceSessionID:STRING, filename:STRING, storeID:STRING, processing_timestamp:TIMESTAMP, file_date:DATE",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            triggering_frequency=300,
            with_auto_sharding=True,
        )
        )

        # Write the processed files (both successfully and unsuccessfully processed) to GCS
        write_to_gcs = (
                files_content_results_gcs
                | "Write to GCS" >> beam.ParDo(MoveFilesToNewGCSBucket())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
