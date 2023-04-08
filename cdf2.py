import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from datetime import datetime
import csv
from io import StringIO
import logging
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'wmt-cc-datasphere-dev-df.json'


def parse_csv(line):
    # skip header row
    if 'cdrRecordType' not in line:
        values = line.split(',')
        record = {'cdrRecordType': values[0], 'globalCallID_callManagerId': values[1], 'globalCallID_callId': values[2],
                  'origLegCallIdentifier': values[3], 'dateTimeOrigination': values[4], 'origNodeId': values[5],
                  'origSpan': values[6], 'origIpAddr': values[7], 'callingPartyNumber': values[8],
                  'callingPartyUnicodeLoginUserID': values[9], 'origCause_location': values[10],
                  'origCause_value': values[11], 'origPrecedenceLevel': values[12],
                  'origMediaTransportAddress_IP': values[13], 'origMediaTransportAddress_Port': values[14],
                  'origMediaCap_payloadCapability': values[15], 'origMediaCap_maxFramesPerPacket': values[16],
                  'origMediaCap_g723BitRate': values[17], 'origVideoCap_Codec': values[18],
                  'origVideoCap_Bandwidth': values[19], 'origVideoCap_Resolution': values[20],
                  'origVideoTransportAddress_IP': values[21], 'origVideoTransportAddress_Port': values[22],
                  'origRSVPAudioStat': values[23], 'origRSVPVideoStat': values[24], 'destLegIdentifier': values[25],
                  'destNodeId': values[26], 'destSpan': values[27], 'destIpAddr': values[28],
                  'originalCalledPartyNumber': values[29], 'finalCalledPartyNumber': values[30],
                  'finalCalledPartyUnicodeLoginUserID': values[31], 'destCause_location': values[32],
                  'destCause_value': values[33], 'destPrecedenceLevel': values[34],
                  'destMediaTransportAddress_IP': values[35], 'destMediaTransportAddress_Port': values[36],
                  'destMediaCap_payloadCapability': values[37], 'destMediaCap_maxFramesPerPacket': values[38],
                  'destMediaCap_g723BitRate': values[39], 'destVideoCap_Codec': values[40],
                  'destVideoCap_Bandwidth': values[41], 'destVideoCap_Resolution': values[42],
                  'destVideoTransportAddress_IP': values[43], 'destVideoTransportAddress_Port': values[44],
                  'destRSVPAudioStat': values[45], 'destRSVPVideoStat': values[46], 'dateTimeConnect': values[47],
                  'dateTimeDisconnect': values[48], 'lastRedirectDn': values[49], 'pkid': values[50],
                  'originalCalledPartyNumberPartition': values[51], 'callingPartyNumberPartition': values[52],
                  'finalCalledPartyNumberPartition': values[53], 'lastRedirectDnPartition': values[54],
                  'duration': values[55], 'origDeviceName': values[56], 'destDeviceName': values[57],
                  'origCallTerminationOnBehalfOf': values[58], 'destCallTerminationOnBehalfOf': values[59],
                  'origCalledPartyRedirectOnBehalfOf': values[60], 'lastRedirectRedirectOnBehalfOf': values[61],
                  'origCalledPartyRedirectReason': values[62], 'lastRedirectRedirectReason': values[63],
                  'destConversationId': values[64], 'globalCallId_ClusterID': values[65], 'joinOnBehalfOf': values[66],
                  'comment': values[67], 'authCodeDescription': values[68], 'authorizationLevel': values[69],
                  'clientMatterCode': values[70], 'origDTMFMethod': values[71], 'destDTMFMethod': values[72],
                  'callSecuredStatus': values[73], 'origConversationId': values[74],
                  'origMediaCap_Bandwidth': values[75], 'destMediaCap_Bandwidth': values[76],
                  'authorizationCodeValue': values[77], 'outpulsedCallingPartyNumber': values[78],
                  'outpulsedCalledPartyNumber': values[79], 'origIpv4v6Addr': values[80], 'destIpv4v6Addr': values[81],
                  'origVideoCap_Codec_Channel2': values[82], 'origVideoCap_Bandwidth_Channel2': values[83],
                  'origVideoCap_Resolution_Channel2': values[84], 'origVideoTransportAddress_IP_Channel2': values[85],
                  'origVideoTransportAddress_Port_Channel2': values[86], 'origVideoChannel_Role_Channel2': values[87],
                  'destVideoCap_Codec_Channel2': values[88], 'destVideoCap_Bandwidth_Channel2': values[89],
                  'destVideoCap_Resolution_Channel2': values[90], 'destVideoTransportAddress_IP_Channel2': values[91],
                  'destVideoTransportAddress_Port_Channel2': values[92], 'destVideoChannel_Role_Channel2': values[93],
                  'IncomingProtocolID': values[94], 'IncomingProtocolCallRef': values[95],
                  'OutgoingProtocolID': values[96], 'OutgoingProtocolCallRef': values[97],
                  'currentRoutingReason': values[98], 'origRoutingReason': values[99],
                  'lastRedirectingRoutingReason': values[100], 'huntPilotPartition': values[101],
                  'huntPilotDN': values[102], 'calledPartyPatternUsage': values[103], 'IncomingICID': values[104],
                  'IncomingOrigIOI': values[105], 'IncomingTermIOI': values[106], 'OutgoingICID': values[107],
                  'OutgoingOrigIOI': values[108], 'OutgoingTermIOI': values[109],
                  'outpulsedOriginalCalledPartyNumber': values[110], 'outpulsedLastRedirectingNumber': values[111],
                  'wasCallQueued': values[112], 'totalWaitTimeInQueue': values[113],
                  'callingPartyNumber_uri': values[114], 'originalCalledPartyNumber_uri': values[115],
                  'finalCalledPartyNumber_uri': values[116], 'lastRedirectDn_uri': values[117],
                  'mobileCallingPartyNumber': values[118], 'finalMobileCalledPartyNumber': values[119],
                  'origMobileDeviceName': values[120], 'destMobileDeviceName': values[121],
                  'origMobileCallDuration': values[122], 'destMobileCallDuration': values[123],
                  'mobileCallType': values[124], 'originalCalledPartyPattern': values[125],
                  'finalCalledPartyPattern': values[126], 'lastRedirectingPartyPattern': values[127],
                  'huntPilotPattern': values[128], 'origDeviceType': values[129], 'destDeviceType': values[130],
                  'origDeviceSessionID': values[131], 'destDeviceSessionID': values[132]
                  }
        return record


def run():
    input_path = "gs://cisco-cdr-dataflow-pipeline-test/cdr_L-SC1_02_202201251216_456988"
    output_table = "wmt-cc-datasphere-dev:ciscocdr.dataflow-pipeline-test3"
    output_path = "gs://cisco-cdr-dataflow-pipeline-output/output"
    poll_interval = 60
    window_size = 300

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).project = 'wmt-cc-datasphere-dev'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).region = 'us-central1'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).job_name = 'cisco-cdr-test-20230405'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).staging_location = 'gs://cisco-cdr-dataflow-pipeline-test/temp02/'
    pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions).temp_location = 'gs://cisco-cdr-dataflow-pipeline-test/temp01/'
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as p:
        lines = (
                p | 'Read Text File' >> beam.io.ReadFromText(input_path, skip_header_lines=2)
        )

        parsed_data = (
                lines | 'Parse CSV' >> beam.Map(parse_csv)
        )

        # Write to BigQuery
        parsed_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema='cdrRecordType:INTEGER,globalCallID_callManagerId:INTEGER,globalCallID_callId:INTEGER,origLegCallIdentifier:INTEGER,dateTimeOrigination:INTEGER,origNodeId:INTEGER,origSpan:INTEGER,origIpAddr:INTEGER,callingPartyNumber:STRING,callingPartyUnicodeLoginUserID:STRING,origCause_location:INTEGER,origCause_value:INTEGER,origPrecedenceLevel:INTEGER,origMediaTransportAddress_IP:INTEGER,origMediaTransportAddress_Port:INTEGER,origMediaCap_payloadCapability:INTEGER,origMediaCap_maxFramesPerPacket:INTEGER,origMediaCap_g723BitRate:INTEGER,origVideoCap_Codec:INTEGER,origVideoCap_Bandwidth:INTEGER,origVideoCap_Resolution:INTEGER,origVideoTransportAddress_IP:INTEGER,origVideoTransportAddress_Port:INTEGER,origRSVPAudioStat:STRING,origRSVPVideoStat:STRING,destLegIdentifier:INTEGER,destNodeId:INTEGER,destSpan:INTEGER,destIpAddr:INTEGER,originalCalledPartyNumber:STRING,finalCalledPartyNumber:STRING,finalCalledPartyUnicodeLoginUserID:STRING,destCause_location:INTEGER,destCause_value:INTEGER,destPrecedenceLevel:INTEGER,destMediaTransportAddress_IP:INTEGER,destMediaTransportAddress_Port:INTEGER,destMediaCap_payloadCapability:INTEGER,destMediaCap_maxFramesPerPacket:INTEGER,destMediaCap_g723BitRate:INTEGER,destVideoCap_Codec:INTEGER,destVideoCap_Bandwidth:INTEGER,destVideoCap_Resolution:INTEGER,destVideoTransportAddress_IP:INTEGER,destVideoTransportAddress_Port:INTEGER,destRSVPAudioStat:STRING,destRSVPVideoStat:STRING,dateTimeConnect:INTEGER,dateTimeDisconnect:INTEGER,lastRedirectDn:STRING,pkid:STRING,originalCalledPartyNumberPartition:STRING,callingPartyNumberPartition:STRING,finalCalledPartyNumberPartition:STRING,lastRedirectDnPartition:STRING,duration:INTEGER,origDeviceName:STRING,destDeviceName:STRING,origCallTerminationOnBehalfOf:INTEGER,destCallTerminationOnBehalfOf:INTEGER,origCalledPartyRedirectOnBehalfOf:INTEGER,lastRedirectRedirectOnBehalfOf:INTEGER,origCalledPartyRedirectReason:INTEGER,lastRedirectRedirectReason:INTEGER,destConversationId:INTEGER,globalCallId_ClusterID:STRING,joinOnBehalfOf:INTEGER,comment:STRING,authCodeDescription:STRING,authorizationLevel:INTEGER,clientMatterCode:STRING,origDTMFMethod:INTEGER,destDTMFMethod:INTEGER,callSecuredStatus:INTEGER,origConversationId:INTEGER,origMediaCap_Bandwidth:INTEGER,destMediaCap_Bandwidth:INTEGER,authorizationCodeValue:STRING,outpulsedCallingPartyNumber:STRING,outpulsedCalledPartyNumber:STRING,origIpv4v6Addr:STRING,destIpv4v6Addr:STRING,origVideoCap_Codec_Channel2:INTEGER,origVideoCap_Bandwidth_Channel2:INTEGER,origVideoCap_Resolution_Channel2:INTEGER,origVideoTransportAddress_IP_Channel2:INTEGER,origVideoTransportAddress_Port_Channel2:INTEGER,origVideoChannel_Role_Channel2:INTEGER,destVideoCap_Codec_Channel2:INTEGER,destVideoCap_Bandwidth_Channel2:INTEGER,destVideoCap_Resolution_Channel2:INTEGER,destVideoTransportAddress_IP_Channel2:INTEGER,destVideoTransportAddress_Port_Channel2:INTEGER,destVideoChannel_Role_Channel2:INTEGER,IncomingProtocolID:INTEGER,IncomingProtocolCallRef:STRING,OutgoingProtocolID:INTEGER,OutgoingProtocolCallRef:STRING,currentRoutingReason:INTEGER,origRoutingReason:INTEGER,lastRedirectingRoutingReason:INTEGER,huntPilotPartition:STRING,huntPilotDN:STRING,calledPartyPatternUsage:INTEGER,IncomingICID:STRING,IncomingOrigIOI:STRING,IncomingTermIOI:STRING,OutgoingICID:STRING,OutgoingOrigIOI:STRING,OutgoingTermIOI:STRING,outpulsedOriginalCalledPartyNumber:STRING,outpulsedLastRedirectingNumber:STRING,wasCallQueued:INTEGER,totalWaitTimeInQueue:INTEGER,callingPartyNumber_uri:STRING,originalCalledPartyNumber_uri:STRING,finalCalledPartyNumber_uri:STRING,lastRedirectDn_uri:STRING,mobileCallingPartyNumber:STRING,finalMobileCalledPartyNumber:STRING,origMobileDeviceName:STRING,destMobileDeviceName:STRING,origMobileCallDuration:INTEGER,destMobileCallDuration:INTEGER,mobileCallType:INTEGER,originalCalledPartyPattern:STRING,finalCalledPartyPattern:STRING,lastRedirectingPartyPattern:STRING,huntPilotPattern:STRING,origDeviceType:STRING,destDeviceType:STRING,origDeviceSessionID:STRING,destDeviceSessionID:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://cisco-cdr-dataflow-pipeline-test/temp01/',
        )

        # Write to GCS
        parsed_data | 'Write to GCS' >> beam.io.WriteToText(output_path)
    logging.info('Job finished!')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
