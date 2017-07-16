#Import libraries
import argparse
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from apiclient.http import MediaFileUpload
from urllib.error import HTTPError
import datetime as dt
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d
import os
import argparse
import getpass

#Define which python file is run
print("Running ga_weekly_download_template")

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
class account_id(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass("Enter the account_id:")
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

parser = argparse.ArgumentParser(description='parse custom_data_source_id,  of the variables')
parser.add_argument('-c', action=custom_data_source_id, nargs='?', dest='custom_data_source_id', help='Enter your custom_data_source_id')
parser.add_argument('-w', action=web_property_id, nargs='?', dest='web_property_id', help='Enter your web_property_id')
parser.add_argument('-a', action=account_id, nargs='?', dest='account_id', help='Enter your account_id')
parser.add_argument('-s', action=service_key_name, nargs='?', dest='service_key_name', help='Enter your service_key_name')
parser.add_argument('-p', action=project_name, nargs='?', dest='project_name', help='Enter your project_name')
args = parser.parse_args()

# Define variables
custom_data_source_id = '{}'.format(args.custom_data_source_id)
web_property_id = '{}'.format(args.web_property_id)
account_id = '{}'.format(args.account_id)
service_account_email = '{}@{}.iam.gserviceaccount.com'.format(args.service_key_name, args.project_name)
this_dir = os.path.dirname(__file__)
key_file_location = os.path.join(this_dir, 'client_secrets.p12')
json_key_file_location = os.path.join(this_dir, 'service_key.json')
'''The parameters below can be adjusted at will:
    - start and end date,
        Define todays date and run for the last week, today should always be Monday 09:00 Bangkok time,
        if not, please change the date.
    - the metrics that are used in the calculation, and
    - the segmentation of the users.
'''
start_day = (dt.date.today() - dt.timedelta(days=7)).strftime('%Y-%m-%d')
end_day = dt.date.today().strftime('%Y-%m-%d') - dt.timedelta(days=1)
no_seg = ['impressions', 'adClicks', 'adCost', 'sessions', 'bounceRate', 'avgSessionDuration', 'pageviews', 'goal6Completions', 'goal11Completions']
seg = ['sessions', 'bounceRate', 'avgSessionDuration', 'pageviews', 'goal6Completions', 'goal11Completions', 'transactions', 'transactionRevenue']
marketing_paid = 'sessions::condition::ga:medium=~^(cpc|ppc|cpa|cpm|cpv|cpp)$'
marketing_free = 'sessions::condition::ga:medium!~^(cpc|ppc|cpa|cpm|cpv|cpp)$'

def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service

def get_first_profile_id(service):
    # Use the Analytics service object to get the first profile id.
    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list( accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')
            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(accountId=account, webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None

# Define the data quering class for no segmentation
class GA_non_segmented(object):
    '''This class can be looped over to query results into a column.
        Need to define service and profile for the class, and
        service, profile, start_day, end_day, and ga:{metric}.'''
    def __init__(self, service, profile_id):
        self.service = service
        self.profile_id = profile_id
    def get_results(self, service, profile_id, start_day, end_day, metric):
        return service.data().ga().get(ids='ga:' + profile_id, start_date=start_day, end_date=end_day, metrics='ga:' + metric).execute()

# Define the data quering class for segmentation
class GA_segmented(object):
    '''This class can be looped over to query results into a column.
        Need to define service and profile for the class, and
        service, profile,  start_day, end_day, ga:{metric}, and segmentation.'''
    def __init__(self, service, profile_id):
        self.service = service
        self.profile_id = profile_id
    def get_results(self, service, profile_id, start_day, end_day, metric, segmentation):
        return service.data().ga().get(ids='ga:' + profile_id, start_date=start_day, end_date=end_day, metrics='ga:' + metric, segment=segmentation).execute()


def main():
    # Define the auth scopes to request.
    scope_ga = ['https://www.googleapis.com/auth/analytics.readonly']
    #Authorize credentials with Google Drive to export data to a Google spreadsheet
    scope_drive = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file_location, scope_drive)
    gc = gspread.authorize(credentials)

    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope_ga, key_file_location, service_account_email)
    profile = get_first_profile_id(service)

    # Modulize the classes
    ga_non_segmented = GA_non_segmented(service, profile)
    ga_segmented = GA_segmented(service, profile)
    # Create an empty pandas dataframe and fill it with this weeks data
    data = pd.DataFrame()
    for metric in no_seg:
        data = data.append(ga_non_segmented.get_results(service, profile, start_day, end_day, metric).get('rows')[0], ignore_index=True)
    for metric in seg:
        data = data.append(ga_segmented.get_results(service, profile, start_day, end_day, metric, marketing_paid).get('rows')[0], ignore_index=True)
    for metric in seg:
        data = data.append(ga_segmented.get_results(service, profile, start_day, end_day, metric, marketing_free).get('rows')[0], ignore_index=True)
    data.columns = [dt.date.today().isocalendar()[1]]
    seg_paid = []
    for word in seg:
        seg_paid.append(word + '_paid')
    seg_free = []
    for word in seg:
        seg_free.append(word + '_free')
    data = data.set_index([no_seg + seg_paid + seg_free])
    
    # Download the existing Gsheet data to a pandas dataframe
    spreadsheet = 'your_googlesheet_id'
    wks_name = 'ga_weekly_data'
    existing_data = g2d.download(gfile=spreadsheet, wks_name=wks_name, col_names=True, row_names=True)
    # Define new DataFrame, calculate and append data
    ga_weekly_data = pd.DataFrame()
    ga_weekly_data = existing_data
    ga_weekly_data[data.columns] = data[data.columns]
    #Upload the whole pandas dataframe to the data dump spreadsheet on worksheet installs
    d2g.upload(ga_weekly_data, spreadsheet, wks_name)

if __name__ == "__main__":
    main()