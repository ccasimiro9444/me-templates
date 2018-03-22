import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#Define which python file is run
print("Running specific_etl")

# start recording time
start_time_script = dt.datetime.now()

def authorize_database(host_name, port_number, database_name, user_name, password_name):
    '''Create a connection a database, if a connect cannot be made an exception will be raised at `conn=`,
    conn.cursor will return a cursor object, you can use this cursor to perform queries.'''
    conn_string = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(
        host_name,
        port_number,
        database_name,
        user_name,
        password_name
        )
    conn = psycopg2.connect(conn_string)
    return conn.cursor()

def authorize_google_drive(json_keyfile_path):
    '''Authorize credentials with Google Drive to export data to a Google spreadsheet'''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('{}/service_key.json'.format(json_keyfile_path), scope)
    gc = gspread.authorize(credentials)
    return gc

def download_google_spreadsheet(spredsheet_url, spreadsheet_tab):
    '''Download the existing google_sheet data to a pandas dataframe'''
    existing_data = g2d.download(gfile=spredsheet_url, wks_name=spreadsheet_tab, col_names=True, row_names=True)
    return existing_data

def upload_google_spreadsheet(pandas_dataframe, spredsheet_url, spreadsheet_tab):
    '''Upload the whole pandas dataframe to the data dump spreadsheet and specific tab '''
    return d2g.upload(pandas_dataframe, gfile=spredsheet_url, wks_name=spreadsheet_tab)

def query(**queries_columns_dict):
    '''Run a query with execute(), and import the table with fetchall()'''
    for value in list(queries_columns_dict.values())[0]:
        cur.execute("{}".format(value))
    df = cur.fetchall()
    df = pd.DataFrame.from_records(df, columns=[value for value in list(queries_columns_dict.values())[1]])
    return df

def main():
    '''Create a connection to your database'''
    cur = authorize_database(
        'host_name',
        '5432',
        'database_name',
        'root',
        'password1234'
        ) #first database
    
    '''
    start a new query.
    define your queries and the resulting column names.
    dont change the variables' names.
    '''
    run_queries = [
        '''first query;'''
        '''
        select user_id
        from (select user_id, count(user_id) as open_count from app_reopenings group by user_id) as temp
        where open_count>1;
        '''
    ]
    column_names = ['user_id']
    #dont change anything
    queries_columns = {'queries': run_queries, 'columns': column_names}
    #define the dataframe name
    users = query(**queries_columns)

    #Disconnect database
    cur.close()
    #conn.close()

    '''Create a connection to your database'''
    cur = authorize_database(
        'host_name',
        '5432',
        'database_name',
        'root',
        'password1234'
        ) #second database

    '''
    start a new query.
    define your queries and the resulting column names.
    dont change the variables' names.
    '''
    run_queries = [
        '''select id, name, email from users where created_at>'2017-06-01';'''
    ]
    column_names = ['user_id', 'name', 'email']
    #dont change anything
    queries_columns = {'queries': run_queries, 'columns': column_names}
    #define the dataframe name
    email_adresses = query(**queries_columns)

    #Disconnect database
    cur.close()
    #conn.close()

    '''
    Merge, but first some transformations.
    '''
    new_users = users[df['user_id'].map(len) < 7].astype(int)
    final_dataset = pd.merge(email_adresses, new_users, how='inner', on='user_id')
    uploader = final_dataset.sample(n=1990, random_state=5)

    '''
    Autorize google.
    Define the path to json_key_file as a string.
    '''
    authorize_google_drive('/Users/my_name/Documents')
    '''
    Upload the dataframe to google spreadsheet.
    Need access to the file, insert dataframe, url-part as string, worksheet name as string
    '''
    upload_google_spreadsheet(uploader, 'some_file', 'some_tab')

if __name__ == "__main__":
    main()
    
