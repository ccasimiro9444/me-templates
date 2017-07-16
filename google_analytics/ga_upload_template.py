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
print("Running ga_upload_template")

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
csv_import_file_location = os.path.join(this_dir, 'my_file.csv')

def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    # Build the service object.
    service = build(api_name, api_version, http=http)
    return service

def uploadCSV(service):
    try:
        media = MediaFileUpload(csv_import_file_location, mimetype='application/octet-stream', resumable=False)
        daily_upload = service.management().uploads().uploadData(accountId=account_id, webPropertyId=web_property_id, customDataSourceId=custom_data_source_id, media_body=media).execute()
    except TypeError as error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : {}'.format(error))

def main():
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/analytics']
    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)
    # Upload CSV Data
    uploadCSV(service)

if __name__ == '__main__':
  main()