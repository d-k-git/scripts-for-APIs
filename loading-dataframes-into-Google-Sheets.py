import googleapiclient.discovery
import argparse
from apiclient import discovery
from oauth2client import client
from oauth2client import tools


SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = "access-v-gs.json"
APPLICATION_NAME = 'Google Sheets API Report'
# a file in which after the authorization procedure tokens are saved
credential_path = 'sheets.googleapis.com-report.json'
#The process should end with a service message: "The authentication flow has completed".

#Сonnect to the required document
from httplib2 import Http
from oauth2client.file import Storage
store = Storage(credential_path)
credentials = store.get()
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, parents=[tools.argparser])
flags = parser.parse_args([])
if not credentials or credentials.invalid:
    flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
    flow.user_agent = APPLICATION_NAME
    if flags:
        credentials = tools.run_flow(flow, store, flags)
    print('Storing credentials to ' + credential_path)


from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from apiclient.discovery import build
from httplib2 import Http


import gspread
from df2gspread import df2gspread as d2g

#https://docs.google.com/spreadsheets/d/1sdoifjalgfg42534dskdUY/edit#gid=0
spreadsheet_key = "1sdoifjalgfg42534dskdUY"

## Write  different dataframes to different pages
wks_name = '_Сampaigns'
d2g.upload(camp_df_1001, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
print('>>>campaigns loaded')
time.sleep(2) 
wks_name = '_Ads'
d2g.upload(df_text_ads_final8, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
print('>>>Ads loaded')
time.sleep(2)
wks_name = 'Parse_time'
d2g.upload(df_time, spreadsheet_key, wks_name, credentials=credentials, row_names=False)
print('>>>Parse_time loaded')
print('DONE!')
