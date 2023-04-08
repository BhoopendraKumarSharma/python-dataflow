import requests
from requests.auth import HTTPBasicAuth
from gcs_manager import *
from osc_config import username, password
from datetime import datetime

# my_cred_file: str = 'service_account.json'
# bucket_name: str = 'bhoopendra_test'
# blob_name: str = 'test_files'
# file_name: str = bucket_name + '_' + str(datetime.now()) + '.txt'
# url = 'https://centralops.custhelp.com/services/rest/connect/v1.4/analyticsreportresults'
#
#
# request_body = {"lookupName": "WMUS_Daily_Counts", "limit": 100, "offset": 0}
# result = requests.post(url,
#                        headers={'Content-Type': 'application/json',
#                                 'OSvC-CREST-Application-Context': 'Include'
#                                 },
#
#                        auth=HTTPBasicAuth(username, password),
#                        json=request_body)
#
#
# print(result.text)
# response = write_string_to_file(my_cred_file, bucket_name, blob_name, file_name, result.text)
# print(response)

# url = 'https://api.incontact.com/InContactAuthorizationServer/Token'
#
#
# request_body = {
#     "grand_type": "password",
#     "password": "VACService21!!!",
#     "scope": None,
#     "username": "api.service@simplexity.com"
# }
# result = requests.post(url,
#                        headers={'Content-Type': 'application/json',
#                                 'Authorization': 'Basic RGF0YUR1cEhvb2sxMkBXYWxtYXJ0SW5jOjQ1OTQ0MjU='
#                                 },
#
#                        auth=HTTPBasicAuth(username, password),
#                        json=request_body)
#
#
# print(result.text)

# url = "http://webservices.oorsprong.org/websamples.countryinfo/CountryInfoService.wso"
#
# # structured XML
# payload = """<?xml version=\"1.0\" encoding=\"utf-8\"?>
#             <soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">
#                 <soap:Body>
#                     <CountryIntPhoneCode xmlns=\"http://www.oorsprong.org/websamples.countryinfo\">
#                         <sCountryISOCode>IN</sCountryISOCode>
#                     </CountryIntPhoneCode>
#                 </soap:Body>
#             </soap:Envelope>"""
# # headers
# headers = {
#     'Content-Type': 'text/xml; charset=utf-8'
# }
# # POST request
# response = requests.request("POST", url, headers=headers, data=payload)
#
# # prints the response
# print(response.text)
# print(response)



url = "https://www.stormportal.us/redviewreportservice/service.svc"

# structured XML
# payload = """<soap:Envelope xmlns:rep="Report" xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
# <soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
#     <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
#     <wsse:UsernameToken wsu:Id="UsernameToken-16946F047C66570E1D145976553375510">
#     <wsse:Username>5265</wsse:Username>
#     <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">sw$Nuv+zuW1f</wsse:Password>
#     </wsse:UsernameToken>
#     </wsse:Security>
#     <wsa:Action>Report/getReport</wsa:Action>
# </soap:Header>
#     <soap:Body>
#         <rep:getReport>
#             <rep:ReportName>Test Report</rep:ReportName>
#             <rep:StartDateTime>2022-05-12 00:00:00</rep:StartDateTime>
#             <rep:EndDateTime>2022-05-12 23:59:59</rep:EndDateTime>
#         </rep:getReport>
#     </soap:Body>
# </soap:Envelope>
# """
# headers

payload = """<soap:Envelope xmlns:rep="Report" xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
   <soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
      <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
         <wsse:UsernameToken wsu:Id="UsernameToken-B9B7D6983EF2C6BA84147992219316811">
            <wsse:Username>5265</wsse:Username>
            <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">sw$Nuv+zuW1f</wsse:Password>
         </wsse:UsernameToken>
      </wsse:Security>
      <wsa:To>https://www.stormportal.us/redviewreportservice/service.svc</wsa:To>
   <wsa:Action>Report/getRealTimeStatistic</wsa:Action>
   </soap:Header>
   <soap:Body>
      <rep:getRealTimeStatistic>
         <rep:Statistic>
            <rep:Type>agentgroup</rep:Type>
            <rep:Identifier>Skill 1</rep:Identifier>
         </rep:Statistic>
      </rep:getRealTimeStatistic>
   </soap:Body>
</soap:Envelope>
"""
headers = {
    'Content-Type': 'text/xml; charset=utf-8'
}
# POST request
response = requests.request("POST", url, headers=headers, data=payload)

# prints the response
print(response.text)
print(response)