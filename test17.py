import logging
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "vivid-argon-376508-f773dd3b2532.json"
PROJECT_ID = "vivid-argon-376508"
SCHEMA = "cdrRecordType:INTEGER, globalCallID_callManagerId:INTEGER, globalCallID_callId:INTEGER, origLegCallIdentifier:INTEGER, dateTimeOrigination:INTEGER, origNodeId:INTEGER, origSpan:INTEGER, origIpAddr:INTEGER, callingPartyNumber:STRING, callingPartyUnicodeLoginUserID:STRING, origCause_location:INTEGER, origCause_value:INTEGER, origPrecedenceLevel:INTEGER, origMediaTransportAddress_IP:INTEGER, origMediaTransportAddress_Port:INTEGER, origMediaCap_payloadCapability:INTEGER, origMediaCap_maxFramesPerPacket:INTEGER, origMediaCap_g723BitRate:INTEGER, origVideoCap_Codec:INTEGER, origVideoCap_Bandwidth:INTEGER, origVideoCap_Resolution:INTEGER, origVideoTransportAddress_IP:INTEGER, origVideoTransportAddress_Port:INTEGER, origRSVPAudioStat:STRING, origRSVPVideoStat:STRING, destLegIdentifier:INTEGER, destNodeId:INTEGER, destSpan:INTEGER, destIpAddr:INTEGER, originalCalledPartyNumber:STRING, finalCalledPartyNumber:STRING, finalCalledPartyUnicodeLoginUserID:STRING, destCause_location:INTEGER, destCause_value:INTEGER, destPrecedenceLevel:INTEGER, destMediaTransportAddress_IP:INTEGER, destMediaTransportAddress_Port:INTEGER, destMediaCap_payloadCapability:INTEGER, destMediaCap_maxFramesPerPacket:INTEGER, destMediaCap_g723BitRate:INTEGER, destVideoCap_Codec:INTEGER, destVideoCap_Bandwidth:INTEGER, destVideoCap_Resolution:INTEGER, destVideoTransportAddress_IP:INTEGER, destVideoTransportAddress_Port:INTEGER, destRSVPAudioStat:STRING, destRSVPVideoStat:STRING, dateTimeConnect:INTEGER, dateTimeDisconnect:INTEGER, lastRedirectDn:STRING, pkid:STRING, originalCalledPartyNumberPartition:STRING, callingPartyNumberPartition:STRING, finalCalledPartyNumberPartition:STRING, lastRedirectDnPartition:STRING, duration:INTEGER, origDeviceName:STRING, destDeviceName:STRING, origCallTerminationOnBehalfOf:INTEGER, destCallTerminationOnBehalfOf:INTEGER, origCalledPartyRedirectOnBehalfOf:INTEGER, lastRedirectRedirectOnBehalfOf:INTEGER, origCalledPartyRedirectReason:INTEGER, lastRedirectRedirectReason:INTEGER, destConversationId:INTEGER, globalCallId_ClusterID:STRING, joinOnBehalfOf:INTEGER, comment:STRING, authCodeDescription:STRING, authorizationLevel:INTEGER, clientMatterCode:STRING, origDTMFMethod:INTEGER, destDTMFMethod:INTEGER, callSecuredStatus:INTEGER, origConversationId:INTEGER, origMediaCap_Bandwidth:INTEGER, destMediaCap_Bandwidth:INTEGER, authorizationCodeValue:STRING, outpulsedCallingPartyNumber:STRING, outpulsedCalledPartyNumber:STRING, origIpv4v6Addr:STRING, destIpv4v6Addr:STRING, origVideoCap_Codec_Channel2:INTEGER, origVideoCap_Bandwidth_Channel2:INTEGER, origVideoCap_Resolution_Channel2:INTEGER, origVideoTransportAddress_IP_Channel2:INTEGER, origVideoTransportAddress_Port_Channel2:INTEGER, origVideoChannel_Role_Channel2:INTEGER, destVideoCap_Codec_Channel2:INTEGER, destVideoCap_Bandwidth_Channel2:INTEGER, destVideoCap_Resolution_Channel2:INTEGER, destVideoTransportAddress_IP_Channel2:INTEGER, destVideoTransportAddress_Port_Channel2:INTEGER, destVideoChannel_Role_Channel2:INTEGER, IncomingProtocolID:INTEGER, IncomingProtocolCallRef:STRING, OutgoingProtocolID:INTEGER, OutgoingProtocolCallRef:STRING, currentRoutingReason:INTEGER, origRoutingReason:INTEGER, lastRedirectingRoutingReason:INTEGER, huntPilotPartition:STRING, huntPilotDN:STRING, calledPartyPatternUsage:INTEGER, IncomingICID:STRING, IncomingOrigIOI:STRING, IncomingTermIOI:STRING, OutgoingICID:STRING, OutgoingOrigIOI:STRING, OutgoingTermIOI:STRING, outpulsedOriginalCalledPartyNumber:STRING, outpulsedLastRedirectingNumber:STRING, wasCallQueued:INTEGER, totalWaitTimeInQueue:INTEGER, callingPartyNumber_uri:STRING, originalCalledPartyNumber_uri:STRING, finalCalledPartyNumber_uri:STRING, lastRedirectDn_uri:STRING, mobileCallingPartyNumber:STRING, finalMobileCalledPartyNumber:STRING, origMobileDeviceName:STRING, destMobileDeviceName:STRING, origMobileCallDuration:INTEGER, destMobileCallDuration:INTEGER, mobileCallType:INTEGER, originalCalledPartyPattern:STRING, finalCalledPartyPattern:STRING, lastRedirectingPartyPattern:STRING, huntPilotPattern:STRING, origDeviceType:STRING, destDeviceType:STRING, origDeviceSessionID:STRING, destDeviceSessionID:STRING, filename:STRING"

class ReadContent(beam.DoFn):
    def process(self, element):
        file_path = element.path
        file_name = file_path.split("/")[-1]
        file_date = file_name.split('_')[3][:8]
        date_str = 'exec_date=' + file_date[:4] + '-' + file_date[4:6] + '-' + file_date[6:8]
        new_file_path = f"{date_str}/{file_name}"
        with beam.io.gcsio.GcsIO().open(file_path, 'r') as f:
            content = f.read().decode('utf-8')
        lines = content.split('\n')
        header_line = lines[0]
        header_fields = header_line.split(',')
        header_fields = [field.strip('"') for field in header_fields]
        lines = lines[2:]
        for line in lines:
            values = line.split(',')
            if len(values) >= len(header_fields):
                # Create a dictionary with the field names as keys and the values as values
                record = {}
                for i in range(len(header_fields)):
                    field_value = values[i].strip('"')
                    if field_value != "":
                        record[header_fields[i]] = field_value
                    else:
                        record[header_fields[i]] = None
                record['filename'] = file_name
                logging.info(record)
                yield {'orig_file_path': file_path, 'new_file_path': new_file_path, 'content': content,
                       'record': record}


class MoveFilesToNewGCSBucket(beam.DoFn):
    def process(self, element):
        orig_file_path = element['orig_file_path']
        new_file_path = element['new_file_path']
        content = element['content']
        output_bucket = "gs://df-data-output"
        output_path = output_bucket + '/' + new_file_path
        with beam.io.gcsio.GcsIO().open(output_path, "w") as f:
            f.write(content.encode('utf-8'))
            # To delete the file from source location; it will fail if permission is not granted for delete
            beam.io.gcsio.GcsIO().delete(orig_file_path)


def count_records(records):
    logging.info(f"Batching {len(records)} records")


class LogElements(beam.DoFn):
    def process(self, element):
        logging.info("Element to write to BigQuery: %s", str(element))
        yield element


def run(argv=None):
    # Hardcode your input parameters here
    input_path = "gs://bks-pipeline-test/cdr*"
    output_table = "vivid-argon-376508.bksdatasets001.cisco-cdr-raw-data"
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

        write_to_bq = (
                files_content
                | "Extract Fields" >> beam.Map(lambda element: element['record'])
                | "Batch Elements" >> beam.BatchElements(min_batch_size=100,
                                                         max_batch_size=200)  # adjust sizes as needed
                | "Flatten Lists" >> beam.FlatMap(lambda batch: batch)
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table=output_table,
                    schema="cdrRecordType:INTEGER, globalCallID_callManagerId:INTEGER, globalCallID_callId:INTEGER, origLegCallIdentifier:INTEGER, dateTimeOrigination:INTEGER, origNodeId:INTEGER, origSpan:INTEGER, origIpAddr:INTEGER, callingPartyNumber:STRING, callingPartyUnicodeLoginUserID:STRING, origCause_location:INTEGER, origCause_value:INTEGER, origPrecedenceLevel:INTEGER, origMediaTransportAddress_IP:INTEGER, origMediaTransportAddress_Port:INTEGER, origMediaCap_payloadCapability:INTEGER, origMediaCap_maxFramesPerPacket:INTEGER, origMediaCap_g723BitRate:INTEGER, origVideoCap_Codec:INTEGER, origVideoCap_Bandwidth:INTEGER, origVideoCap_Resolution:INTEGER, origVideoTransportAddress_IP:INTEGER, origVideoTransportAddress_Port:INTEGER, origRSVPAudioStat:STRING, origRSVPVideoStat:STRING, destLegIdentifier:INTEGER, destNodeId:INTEGER, destSpan:INTEGER, destIpAddr:INTEGER, originalCalledPartyNumber:STRING, finalCalledPartyNumber:STRING, finalCalledPartyUnicodeLoginUserID:STRING, destCause_location:INTEGER, destCause_value:INTEGER, destPrecedenceLevel:INTEGER, destMediaTransportAddress_IP:INTEGER, destMediaTransportAddress_Port:INTEGER, destMediaCap_payloadCapability:INTEGER, destMediaCap_maxFramesPerPacket:INTEGER, destMediaCap_g723BitRate:INTEGER, destVideoCap_Codec:INTEGER, destVideoCap_Bandwidth:INTEGER, destVideoCap_Resolution:INTEGER, destVideoTransportAddress_IP:INTEGER, destVideoTransportAddress_Port:INTEGER, destRSVPAudioStat:STRING, destRSVPVideoStat:STRING, dateTimeConnect:INTEGER, dateTimeDisconnect:INTEGER, lastRedirectDn:STRING, pkid:STRING, originalCalledPartyNumberPartition:STRING, callingPartyNumberPartition:STRING, finalCalledPartyNumberPartition:STRING, lastRedirectDnPartition:STRING, duration:INTEGER, origDeviceName:STRING, destDeviceName:STRING, origCallTerminationOnBehalfOf:INTEGER, destCallTerminationOnBehalfOf:INTEGER, origCalledPartyRedirectOnBehalfOf:INTEGER, lastRedirectRedirectOnBehalfOf:INTEGER, origCalledPartyRedirectReason:INTEGER, lastRedirectRedirectReason:INTEGER, destConversationId:INTEGER, globalCallId_ClusterID:STRING, joinOnBehalfOf:INTEGER, comment:STRING, authCodeDescription:STRING, authorizationLevel:INTEGER, clientMatterCode:STRING, origDTMFMethod:INTEGER, destDTMFMethod:INTEGER, callSecuredStatus:INTEGER, origConversationId:INTEGER, origMediaCap_Bandwidth:INTEGER, destMediaCap_Bandwidth:INTEGER, authorizationCodeValue:STRING, outpulsedCallingPartyNumber:STRING, outpulsedCalledPartyNumber:STRING, origIpv4v6Addr:STRING, destIpv4v6Addr:STRING, origVideoCap_Codec_Channel2:INTEGER, origVideoCap_Bandwidth_Channel2:INTEGER, origVideoCap_Resolution_Channel2:INTEGER, origVideoTransportAddress_IP_Channel2:INTEGER, origVideoTransportAddress_Port_Channel2:INTEGER, origVideoChannel_Role_Channel2:INTEGER, destVideoCap_Codec_Channel2:INTEGER, destVideoCap_Bandwidth_Channel2:INTEGER, destVideoCap_Resolution_Channel2:INTEGER, destVideoTransportAddress_IP_Channel2:INTEGER, destVideoTransportAddress_Port_Channel2:INTEGER, destVideoChannel_Role_Channel2:INTEGER, IncomingProtocolID:INTEGER, IncomingProtocolCallRef:STRING, OutgoingProtocolID:INTEGER, OutgoingProtocolCallRef:STRING, currentRoutingReason:INTEGER, origRoutingReason:INTEGER, lastRedirectingRoutingReason:INTEGER, huntPilotPartition:STRING, huntPilotDN:STRING, calledPartyPatternUsage:INTEGER, IncomingICID:STRING, IncomingOrigIOI:STRING, IncomingTermIOI:STRING, OutgoingICID:STRING, OutgoingOrigIOI:STRING, OutgoingTermIOI:STRING, outpulsedOriginalCalledPartyNumber:STRING, outpulsedLastRedirectingNumber:STRING, wasCallQueued:INTEGER, totalWaitTimeInQueue:INTEGER, callingPartyNumber_uri:STRING, originalCalledPartyNumber_uri:STRING, finalCalledPartyNumber_uri:STRING, lastRedirectDn_uri:STRING, mobileCallingPartyNumber:STRING, finalMobileCalledPartyNumber:STRING, origMobileDeviceName:STRING, destMobileDeviceName:STRING, origMobileCallDuration:INTEGER, destMobileCallDuration:INTEGER, mobileCallType:INTEGER, originalCalledPartyPattern:STRING, finalCalledPartyPattern:STRING, lastRedirectingPartyPattern:STRING, huntPilotPattern:STRING, origDeviceType:STRING, destDeviceType:STRING, origDeviceSessionID:STRING, destDeviceSessionID:STRING, filename:STRING",
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    triggering_frequency=300,
                    with_auto_sharding=True,
                )
        )


        write_to_gcs = (
                files_content
                | "Write to GCS" >> beam.ParDo(MoveFilesToNewGCSBucket())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
