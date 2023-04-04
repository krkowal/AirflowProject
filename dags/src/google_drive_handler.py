import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']


# TODO learn about packages and modules with __init__
def _check_credentials():
    creds = None
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
    service = build('drive', 'v3', credentials=creds)
    return service


def send_image_from_disk(file_name, mimetype='image/jpg', ):
    _send_file_from_disk(
        file_metadata={
            'name': file_name,
        },
        path=f'images/{file_name}',
        mimetype=mimetype
    )


def send_csv_from_disk(file_name):
    _send_file_from_disk(
        file_metadata={
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        },
        path=f'csv/{file_name}',
        mimetype='text/csv'
    )


def _send_file_from_disk(file_metadata, path, mimetype):
    # TODO add possible folder path to root and then create folders or detect if folders exist
    try:
        service = _check_credentials()

        media = MediaFileUpload(f'/opt/airflow/{path}',
                                mimetype=mimetype)

        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='id').execute()
        print(F'File ID: {file.get("id")}')

    except HttpError as error:
        print(f'An error occurred: {error}')


def debug():
    # try:
    #     service = _check_credentials()
    # types = service.files().get
    pass


def create_google_drive_folder(folder_name):
    service = _check_credentials()
    folder_metadata = {
        'name': folder_name,
        'mimeType': "application/vnd.google-apps.folder"
    }
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    return folder.get('id')


def folder_exists(folder_name):
    service = _check_credentials()
    # TODO check if folder exists
    folder = service.files().list(q="mimeType='application/vnd.google-apps.folder'",
                                  spaces="drive",
                                  )


