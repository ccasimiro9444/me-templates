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
import os
import argparse
import getpass

#Define which python file is run
print("Running ga_download_template")

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

#Define variables
custom_data_source_id = '{}'.format(args.custom_data_source_id)
web_property_id = '{}'.format(args.web_property_id)
account_id = '{}'.format(args.account_id)
service_account_email = '{}@{}.iam.gserviceaccount.com'.format(args.service_key_name, args.project_name)
this_dir = os.path.dirname(__file__)
key_file_location = os.path.join(this_dir, 'client_secrets.p12')
#csv_import_file_location = os.path.join(this_dir, 'my_file.csv') -only for upload

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

def get_results(service, profile_id):
    # Use the Analytics Service Object to query the Core Reporting API
    # for the number of sessions within the past seven days.
    return service.data().ga().get(ids='ga:' + profile_id, start_date='7daysAgo', end_date='today', metrics='ga:sessions').execute()

def print_results(results):
    # Print data nicely for the user.
    if results:
        print ('View (Profile): {}'.format(results.get('profileInfo').get('profileName')))
        print ('Total Sessions: {}'.format(results.get('rows')[0][0]))

    else:
        print ('No results found')

def main():
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']

    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)
    profile = get_first_profile_id(service)
    print_results(get_results(service, profile))

if __name__ == '__main__':
  main()