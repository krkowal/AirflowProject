from __future__ import print_function

import os.path
from datetime import timedelta, datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from airflow.decorators import dag, task

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=3)
}


@dag(
    dag_id='send_file_to_google_drive',
    tags=['google_drive'],
    default_args=default_args,
    start_date=datetime(2023, 4, 4),
    catchup=False,
    schedule_interval='@daily'
)
def send_file_to_google_drive():
    @task()
    def send_file():
        """Shows basic usage of the Drive v3 API.
        Prints the names and ids of the first 10 files the user has access to.
        """
        creds = None

        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.

        # def list_files(startpath):
        #     for root, dirs, files in os.walk(startpath):
        #         level = root.replace(startpath, '').count(os.sep)
        #         indent = ' ' * 4 * (level)
        #         print('{}{}/'.format(indent, os.path.basename(root)))
        #         subindent = ' ' * 4 * (level + 1)
        #         for f in files:
        #             print('{}{}'.format(subindent, f))
        #
        # list_files('/opt/airflow/')

        if os.path.exists('/opt/airflow/auth/token.json'):
            creds = Credentials.from_authorized_user_file('/opt/airflow/auth/token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    '/opt/airflow/auth/credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('/opt/airflow/auth/token.json', 'w') as token:
                token.write(creds.to_json())

        try:
            service = build('drive', 'v3', credentials=creds)

            # Call the Drive v3 API
            # results = service.files().list(
            #     pageSize=10, fields="nextPageToken, files(id, name)").execute()
            # items = results.get('files', [])
            # media = MediaFileUpload('text.txt', mimetype='image/jpg')
            # file_metadata = {'name': 'kotek.jpg'}
            # service.files().create(media_body=media, body=file_metadata, fields='id').execute()

            file_metadata = {'name': 'kotek.jpeg'}
            media = MediaFileUpload('/opt/airflow/images/kotek.jpg',
                                    mimetype='image/jpeg')
            # pylint: disable=maybe-no-member
            file = service.files().create(body=file_metadata, media_body=media,
                                          fields='id').execute()
            print(F'File ID: {file.get("id")}')

            # if not items:
            #     print('No files found.')
            #     return
            # print('Files:')
            # for item in items:
            #     print(u'{0} ({1})'.format(item['name'], item['id']))
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f'An error occurred: {error}')

    send_file()


task = send_file_to_google_drive()
