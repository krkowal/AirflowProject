import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

EXTENSION_MAPPER = {
    'jpg': 'images',
    'csv': 'csv',
}


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
    """Cannot concurrently create 2 folders of the same name.
    If it happens, there will be 2 folders of the same name and each file will be in separate folder"""
    _send_file_from_disk(
        file_metadata={
            'name': file_name,
        },
        path=file_name,
        mimetype=mimetype
    )


def send_csv_from_disk(file_name):
    """Cannot concurrently create 2 folders of the same name.
    If it happens, there will be 2 folders of the same name and each file will be in separate folder"""
    _send_file_from_disk(
        file_metadata={
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        },
        path=file_name,
        mimetype='text/csv'
    )


def _send_file_from_disk(file_metadata, path, mimetype):
    try:
        service = _check_credentials()
        folder_path, file_name = __split_path(path)

        parents = []
        if folder_path is not None:
            for folder in folder_path:
                folder_id = _folder_exists(folder)
                if folder_id is None:
                    folder_id = create_google_drive_folder(folder, parents)
                parents = [folder_id]

        file_metadata_with_parents = file_metadata
        file_metadata_with_parents['parents'] = parents
        file_metadata_with_parents['name'] = file_name
        media = MediaFileUpload(f'/opt/airflow/{EXTENSION_MAPPER[file_name.split(".")[1]]}/{file_name}',
                                mimetype=mimetype)

        file = service.files().create(body=file_metadata_with_parents, media_body=media,
                                      fields='id').execute()
        print(F'File ID: {file.get("id")}')

    except HttpError as error:
        raise HttpError(f'An error occurred: {error}')


def __split_path(path):
    paths = path.split("/")
    if len(paths) == 1:
        return None, paths[0]
    else:
        return paths[:-1], paths[-1]


def debug():
    pass


def create_google_drive_folder(folder_name, parents):
    service = _check_credentials()
    folder_metadata = {
        'name': folder_name,
        'mimeType': "application/vnd.google-apps.folder",
        'parents': parents
    }
    folder = service.files().create(body=folder_metadata, fields='id,parents').execute()
    return folder.get('id')


def _folder_exists(folder_name):
    service = _check_credentials()
    page_token = None
    response = service.files().list(
        q=f"mimeType='application/vnd.google-apps.folder' and name = '{folder_name}' and trashed = false",
        spaces="drive",
        fields='nextPageToken, '
               'files(id, name)',
        pageToken=page_token
    ).execute()
    folders = response.get('files', [])
    if len(folders) == 0:
        return None
    else:
        return folders[0].get('id')
