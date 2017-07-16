#Import libraries
from bingads.service_client import ServiceClient
from bingads.authorization import *
from bingads.reporting import ReportingServiceManager, ReportingDownloadParameters, ReportingDownloadOperation
from bingads.v11.reporting import ReportingServiceManager
import sys
import os
import webbrowser
from time import gmtime, strftime
from suds import WebFault
import pandas as pd
import numpy as np
import datetime as dt
import argparse
import getpass
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from apiclient.http import MediaFileUpload
from urllib.error import HTTPError

#Define which python file is run
print("Running bing_daily_ga_transfer_template")

class custom_data_source_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the custom_data_source_id:")
        setattr(namespace, self.dest, values)
class web_property_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the web_property_id:")
        setattr(namespace, self.dest, values)
class account_id_google(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the account_id_google:")
        setattr(namespace, self.dest, values)
class service_key_name(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the service_key_name:")
        setattr(namespace, self.dest, values)
class project_name(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the project_name:")
        setattr(namespace, self.dest, values)
class developer_token(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the developer_token:")
        setattr(namespace, self.dest, values)
class client_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the client_id:")
        setattr(namespace, self.dest, values)
class account_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the Bing account_id:")
        setattr(namespace, self.dest, values)
class customer_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the customer_id:")
        setattr(namespace, self.dest, values)

parser = argparse.ArgumentParser(description='parse various variables')
parser.add_argument('-c', action=custom_data_source_id, nargs='?', dest='custom_data_source_id', help='Enter your custom_data_source_id')
parser.add_argument('-w', action=web_property_id, nargs='?', dest='web_property_id', help='Enter your web_property_id')
parser.add_argument('-a', action=account_id_google, nargs='?', dest='account_id_google', help='Enter your account_id_google')
parser.add_argument('-s', action=service_key_name, nargs='?', dest='service_key_name', help='Enter your service_key_name')
parser.add_argument('-p', action=project_name, nargs='?', dest='project_name', help='Enter your project_name')
parser.add_argument('-d', action=developer_token, nargs='?', dest='developer_token', help='Enter your developer_token')
parser.add_argument('-b', action=client_id, nargs='?', dest='client_id', help='Enter your client_id')
parser.add_argument('-a', action=account_id, nargs='?', dest='account_id', help='Enter your Bing account_id')
parser.add_argument('-a', action=customer_id, nargs='?', dest='customer_id', help='Enter your customer_id')
args = parser.parse_args()

#The first main is for the variables, while the main at the end the functions executes.

if __name__ == '__main__':
    print("Python loads the web service proxies at runtime, so you will observe " \
          "a performance delay between program launch and main execution...\n")
    
    '''For other types of reports go on
    https://github.com/BingAds/BingAds-Python-SDK/blob/master/examples/BingAdsPythonConsoleExamples/BingAdsPythonConsoleExamples/v9/ReportRequests.py'''

    #Define parameters to define for Bing
    DEVELOPER_TOKEN = '{}'.format(args.developer_token)
    ENVIRONMENT = 'production'
    FILE_DIRECTORY = os.path.dirname(__file__)
    FILE_NAME = 'bing.csv'
    REPORT_FILE_FORMAT = 'Csv'
    TIMEOUT_IN_MILLISECONDS = 3600000
    #Define parameters to define for Google
    custom_data_source_id = '{}'.format(args.custom_data_source_id)
    web_property_id = '{}'.format(args.web_property_id)
    account_id_google = '{}'.format(args.account_id_google)
    service_account_email = '{}@{}.iam.gserviceaccount.com'.format(args.service_key_name, args.project_name)
    key_file_location = os.path.join(FILE_DIRECTORY, 'client_secrets.p12')
    csv_import_file_location = os.path.join(FILE_DIRECTORY, FILE_NAME)
    # If you are using OAuth in production, CLIENT_ID is required and CLIENT_STATE is recommended.
    CLIENT_ID = '{}'.format(args.client_id)
    CLIENT_STATE = 'ClientStateGoesHere' #works without it

    authorization_data=AuthorizationData(
        account_id='{}'.format(args.account_id),
        customer_id='{}'.format(args.customer_id),
        developer_token=DEVELOPER_TOKEN,
        authentication='OAuthWebAuthCodeGrant',
    )

    campaign_service=ServiceClient(
        service='CampaignManagementService', 
        authorization_data=authorization_data, 
        environment=ENVIRONMENT,
        version=10,
    )

    customer_service=ServiceClient(
        'CustomerManagementService', 
        authorization_data=authorization_data, 
        environment=ENVIRONMENT,
        version=9,
    )

    reporting_service_manager=ReportingServiceManager(
        authorization_data=authorization_data, 
        poll_interval_in_milliseconds=5000, 
        environment=ENVIRONMENT,
    )

    # In addition to ReportingServiceManager, you will need a reporting ServiceClient 
    # to build the ReportRequest.

    reporting_service=ServiceClient(
        'ReportingService', 
        authorization_data=authorization_data, 
        environment=ENVIRONMENT,
        version=11,
    )

'''def authenticate_with_username():
    
    #Sets the authentication property of the global AuthorizationData instance with PasswordAuthentication.
    
    global authorization_data
    authentication=PasswordAuthentication(
        user_name='email@url.com',
        password='your_password_if_not_using_oauth'
    )

    # Assign this authentication instance to the global authorization_data. 
    authorization_data.authentication=authentication
'''
 
def authenticate_with_oauth():
    ''' 
    Sets the authentication property of the global AuthorizationData instance with OAuthDesktopMobileAuthCodeGrant.
    '''
    global authorization_data

    authentication=OAuthDesktopMobileAuthCodeGrant(
        client_id=CLIENT_ID
    )

    # It is recommended that you specify a non guessable 'state' request parameter to help prevent
    # cross site request forgery (CSRF). 
    authentication.state=CLIENT_STATE

    # Assign this authentication instance to the global authorization_data. 
    authorization_data.authentication=authentication   

    # Register the callback function to automatically save the refresh token anytime it is refreshed.
    # Uncomment this line if you want to store your refresh token. Be sure to save your refresh token securely.
    authorization_data.authentication.token_refreshed_callback=save_refresh_token
    
    refresh_token=get_refresh_token()
    
    try:
        # If we have a refresh token let's refresh it
        if refresh_token is not None:
            authorization_data.authentication.request_oauth_tokens_by_refresh_token(refresh_token)
        else:
            request_user_consent()
    except OAuthTokenRequestException:
        # The user could not be authenticated or the grant is expired. 
        # The user must first sign in and if needed grant the client application access to the requested scope.
        request_user_consent()
    
def request_user_consent():
    global authorization_data

    webbrowser.open(authorization_data.authentication.get_authorization_endpoint(), new=1)
    # For Python 3.x use 'input' instead of 'raw_input'
    if(sys.version_info.major >= 3):
        response_uri=input(
            "You need to provide consent for the application to access your Bing Ads accounts. " \
            "After you have granted consent in the web browser for the application to access your Bing Ads accounts, " \
            "please enter the response URI that includes the authorization 'code' parameter: \n"
        )
    else:
        response_uri=raw_input(
            "You need to provide consent for the application to access your Bing Ads accounts. " \
            "After you have granted consent in the web browser for the application to access your Bing Ads accounts, " \
            "please enter the response URI that includes the authorization 'code' parameter: \n"
        )

    if authorization_data.authentication.state != CLIENT_STATE:
       raise Exception("The OAuth response state does not match the client request state.")

    # Request access and refresh tokens using the URI that you provided manually during program execution.
    authorization_data.authentication.request_oauth_tokens_by_response_uri(response_uri=response_uri) 

def get_refresh_token():
    ''' 
    Returns a refresh token if stored locally.
    '''
    file=None
    try:
        file=open("refresh.txt")
        line=file.readline()
        file.close()
        return line if line else None
    except IOError:
        if file:
            file.close()
        return None

def save_refresh_token(oauth_tokens):
    ''' 
    Stores a refresh token locally. Be sure to save your refresh token securely.
    '''
    with open("refresh.txt","w+") as file:
        file.write(oauth_tokens.refresh_token)
        file.close()
    return None

def search_accounts_by_user_id(user_id):
    ''' 
    Search for account details by UserId.
    
    :param user_id: The Bing Ads user identifier.
    :type user_id: long
    :return: List of accounts that the user can manage.
    :rtype: ArrayOfAccount
    '''
    global customer_service
   
    paging={
        'Index': 0,
        'Size': 10
    }

    predicates={
        'Predicate': [
            {
                'Field': 'UserId',
                'Operator': 'Equals',
                'Value': user_id,
            },
        ]
    }

    search_accounts_request={
        'PageInfo': paging,
        'Predicates': predicates
    }
        
    return customer_service.SearchAccounts(
        PageInfo = paging,
        Predicates = predicates
    )

def output_status_message(message):
    print(message)

def set_elements_to_none(suds_object):
    # Bing Ads Campaign Management service operations require that if you specify a non-primitives, 
    # it must be one of the values defined by the service i.e. it cannot be a nil element. 
    # Since Suds requires non-primitives and Bing Ads won't accept nil elements in place of an enum value, 
    # you must either set the non-primitives or they must be set to None. Also in case new properties are added 
    # in a future service release, it is a good practice to set each element of the SUDS object to None as a baseline. 

    for (element) in suds_object:
        suds_object.__setitem__(element[0], None)
    return suds_object

def get_keyword_performance_report_request():
    '''
    Build a keyword performance report request, including Format, ReportName, Aggregation,
    Scope, Time, Filter, and Columns.
    '''
    report_request=reporting_service.factory.create('KeywordPerformanceReportRequest')
    report_request.ExcludeColumnHeaders = None
    report_request.ExcludeReportFooter = True
    report_request.ExcludeReportHeader = True
    report_request.Format=REPORT_FILE_FORMAT
    report_request.ReportName='My Keyword Performance Report'
    report_request.ReturnOnlyCompleteData=True
    report_request.Aggregation='Daily'
    report_request.Language='English'

    scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')
    scope.AccountIds={'long': [authorization_data.account_id] }
    scope.Campaigns= None
    scope.AdGroups= None
    report_request.Scope=scope

    report_time=reporting_service.factory.create('ReportTime')
    # You may either use a custom date range or predefined time.
    
    #custom_date_range_start=reporting_service.factory.create('Date')
    #custom_date_range_start.Day=1
    #custom_date_range_start.Month=1
    #custom_date_range_start.Year=int(strftime("%Y", gmtime()))-1
    #report_time.CustomDateRangeStart=custom_date_range_start
    #custom_date_range_end=reporting_service.factory.create('Date')
    #custom_date_range_end.Day=31
    #custom_date_range_end.Month=12
    #custom_date_range_end.Year=int(strftime("%Y", gmtime()))-1
    #report_time.CustomDateRangeEnd=custom_date_range_end
    #report_time.PredefinedTime=None
    
    report_time.PredefinedTime='Yesterday'
    report_request.Time=report_time

    # If you specify a filter, results may differ from data you see in the Bing Ads web application

    #report_filter=reporting_service.factory.create('KeywordPerformanceReportFilter')
    #report_filter.DeviceType=[
    #    'Computer',
    #    'SmartPhone'
    #]
    #report_request.Filter=report_filter

    # Specify the attribute and data report columns.

    report_columns=reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
    report_columns.KeywordPerformanceReportColumn.append([
        'TimePeriod',
        'CampaignName',
        'Keyword',
        'Clicks',
        'Spend',
        'Impressions',
    ])
    report_request.Columns=report_columns

    # You may optionally sort by any KeywordPerformanceReportColumn, and optionally
    # specify the maximum number of rows to return in the sorted report. 

    report_sorts=reporting_service.factory.create('ArrayOfKeywordPerformanceReportSort')
    report_sort=reporting_service.factory.create('KeywordPerformanceReportSort')
    report_sort.SortColumn='Clicks'
    report_sort.SortOrder='Ascending'
    report_sorts.KeywordPerformanceReportSort.append(report_sort)
    report_request.Sort=report_sorts

    report_request.MaxRows=10

    return report_request

def background_completion(reporting_download_parameters):
    '''
    You can submit a download request and the ReportingServiceManager will automatically 
    return results. The ReportingServiceManager abstracts the details of checking for result file 
    completion, and you don't have to write any code for results polling.
    '''
    global reporting_service_manager
    result_file_path = reporting_service_manager.download_file(reporting_download_parameters)
    output_status_message("Download result file: {0}\n".format(result_file_path))

def submit_and_download(report_request):
    '''
    Submit the download request and then use the ReportingDownloadOperation result to 
    track status until the report is complete e.g. either using
    ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status().
    '''
    global reporting_service_manager
    reporting_download_operation = reporting_service_manager.submit_download(report_request)

    # You may optionally cancel the track() operation after a specified time interval.
    reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)

    # You can use ReportingDownloadOperation.track() to poll until complete as shown above, 
    # or use custom polling logic with get_status() as shown below.
    #for i in range(10):
    #    time.sleep(reporting_service_manager.poll_interval_in_milliseconds / 1000.0)

    #    download_status = reporting_download_operation.get_status()
        
    #    if download_status.status == 'Success':
    #        break
    
    result_file_path = reporting_download_operation.download_result_file(
        result_file_directory = FILE_DIRECTORY, 
        result_file_name = FILE_NAME, 
        decompress = True, 
        overwrite = True,  # Set this value true if you want to overwrite the same file.
        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
    )
    
    output_status_message("Download result file: {0}\n".format(result_file_path))

def download_results(request_id, authorization_data):
    '''
    If for any reason you have to resume from a previous application state, 
    you can use an existing download request identifier and use it 
    to download the result file. Use ReportingDownloadOperation.track() to indicate that the application 
    should wait to ensure that the download status is completed.
    '''
    reporting_download_operation = ReportingDownloadOperation(
        request_id = request_id, 
        authorization_data=authorization_data, 
        poll_interval_in_milliseconds=1000, 
        environment=ENVIRONMENT,
    )

    # Use track() to indicate that the application should wait to ensure that 
    # the download status is completed.
    # You may optionally cancel the track() operation after a specified time interval.
    reporting_operation_status = reporting_download_operation.track(timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS)
    
    result_file_path = reporting_download_operation.download_result_file(
        result_file_directory = FILE_DIRECTORY, 
        result_file_name = FILE_NAME, 
        decompress = True, 
        overwrite = True,  # Set this value true if you want to overwrite the same file.
        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
    ) 

    output_status_message("Download result file: {0}".format(result_file_path))
    output_status_message("Status: {0}\n".format(reporting_operation_status.status))

def get_csv_as_pandas(bing_data_file_name):
    column_names = ['ga:date', 'ga:campaign', 'ga:keyword', 'ga:adClicks', 'ga:adCost', 'ga:impressions']
    bing_data = pd.read_csv(bing_data_file_name, sep=',', header=0, names=column_names)
    source_medium = pd.DataFrame(
        {'ga:source': ['bing'] * len(bing_data),
        'ga:medium': ['cpc'] * len(bing_data)
        }, columns=['ga:source', 'ga:medium'])
    bing_data = pd.concat([bing_data.iloc[:,0:2], source_medium, bing_data.iloc[:,2:]], axis=1)
    bing_data.iloc[:,0] = bing_data.iloc[:,0].str.replace('-', '')
    bing_data.iloc[:,4] = bing_data.iloc[:,4].str.replace('+', '')
    return bing_data

def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service

def uploadCSV(service):
    try:
        media = MediaFileUpload(csv_import_file_location, mimetype='application/octet-stream', resumable=False)
        daily_upload = service.management().uploads().uploadData(accountId=account_id_google, webPropertyId=web_property_id, customDataSourceId=custom_data_source_id, media_body=media).execute()
    except TypeError as error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : {}'.format(error))

# Main execution
if __name__ == '__main__':

    try:
        # You should authenticate for Bing Ads production services with a Microsoft Account, 
        # instead of providing the Bing Ads username and password set. 
        # Authentication with a Microsoft Account is currently not supported in Sandbox.
        authenticate_with_oauth()
        
        # Uncomment to run with Bing Ads legacy UserName and Password credentials.
        # For example you would use this method to authenticate in sandbox.
        #authenticate_with_username()
        
        # Set to an empty user identifier to get the current authenticated Bing Ads user,
        # and then search for all accounts the user may access.
        user=customer_service.GetUser(None).User
        accounts=search_accounts_by_user_id(user.Id)

        # For this example we'll use the first account.
        authorization_data.account_id=accounts['Account'][0].Id
        authorization_data.customer_id=accounts['Account'][0].ParentCustomerId

        # You can submit one of the example reports, or build your own.

        #report_request=get_budget_summary_report_request()
        #report_request=get_user_location_performance_report_request()
        report_request=get_keyword_performance_report_request()
        
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory = FILE_DIRECTORY, 
            result_file_name = FILE_NAME, 
            overwrite_result_file = True, # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS # You may optionally cancel the download after a specified time interval.
        )

        #Option A - Background Completion with ReportingServiceManager
        #You can submit a download request and the ReportingServiceManager will automatically 
        #return results. The ReportingServiceManager abstracts the details of checking for result file 
        #completion, and you don't have to write any code for results polling.

        output_status_message("Awaiting Background Completion . . .");
        background_completion(reporting_download_parameters)

        #Option B - Submit and Download with ReportingServiceManager
        #Submit the download request and then use the ReportingDownloadOperation result to 
        #track status yourself using ReportingServiceManager.get_status().

        output_status_message("Awaiting Submit and Download . . .");
        submit_and_download(report_request)

        #Option C - Download Results with ReportingServiceManager
        #If for any reason you have to resume from a previous application state, 
        #you can use an existing download request identifier and use it 
        #to download the result file. 

        #For example you might have previously retrieved a request ID using submit_download.
        reporting_operation=reporting_service_manager.submit_download(report_request);
        request_id=reporting_operation.request_id;

        #Given the request ID above, you can resume the workflow and download the report.
        #The report request identifier is valid for two days. 
        #If you do not download the report within two days, you must request the report again.
        output_status_message("Awaiting Download Results . . .");
        download_results(request_id, authorization_data)

        output_status_message("Bing program execution completed")

        bing_data_in_pandas = get_csv_as_pandas(os.path.join(FILE_DIRECTORY, FILE_NAME))
        output_status_message("Data cleaned, awaiting upload to Google Analytics")
        bing_data_in_pandas.to_csv(os.path.join(FILE_DIRECTORY, FILE_NAME), sep=',', index=False)

        # Define the auth scopes to request.
        scope = ['https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/analytics']
        # Authenticate and construct service.
        service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)
        # Upload CSV Data
        uploadCSV(service)

        output_status_message("Google Analytics upload completed")

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)
