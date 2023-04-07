from __future__ import annotations

import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

EXTENSION_TO_AIRFLOW_FOLDER_MAPPER: dict[str, str] = {
    'jpg': 'images',
    'csv': 'csv',
}

EXTENSION_TO_MIMETYPE_MAPPER: dict[str, str] = {
    'folder': 'application/vnd.google-apps.folder',
    'jpg': 'image/jpeg',
    'csv': 'application/vnd.google-apps.spreadsheet'
}

MIMETYPES_TO_BE_EXTENSION_STRIPPED: list[str] = ['application/vnd.google-apps.spreadsheet', ]


# TODO learn about packages and modules with __init__
def _check_credentials():
    """
    returns service which connects with Google Drive
    """
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


def send_image_from_disk(file_name: str, mimetype: str = 'image/jpg', ) -> None:
    """Cannot concurrently create 2 folders of the same name.
    If it happens, there will be 2 folders of the same name and each file will be in separate folder"""
    _send_file_from_disk(
        file_metadata={
            'name': file_name,
        },
        path=file_name,
        mimetype=mimetype
    )


def send_csv_from_disk(file_name: str) -> None:
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


def _send_file_from_disk(file_metadata: dict[str, str | list[str]], path: str, mimetype: str) -> None:
    try:
        service = _check_credentials()
        folder_path: str | None
        file_name: str
        folder_path, file_name = __split_path(path)

        parents: list[str] = []
        if folder_path is not None:
            for folder in folder_path:
                if len(parents) != 0:
                    folder_id = _file_exists(folder, parents[0])
                else:
                    folder_id = _file_exists(folder, None)
                if folder_id is None:
                    folder_id = create_google_drive_folder(folder, parents)
                parents = [folder_id]

        file_metadata_with_parents = file_metadata
        file_metadata_with_parents['parents'] = parents
        file_metadata_with_parents['name'] = file_name
        media = MediaFileUpload(
            f'/opt/airflow/{EXTENSION_TO_AIRFLOW_FOLDER_MAPPER[file_name.split(".")[1]]}/{file_name}',
            mimetype=mimetype)

        file = service.files().create(body=file_metadata_with_parents, media_body=media,
                                      fields='id').execute()
        print(F'File ID: {file.get("id")}')

    except HttpError as error:
        raise HttpError(f'An error occurred: {error}')


def __split_path(path: str) -> tuple[list[str] | None, str]:
    paths: list[str] = path.split("/")
    if len(paths) == 1:
        return None, paths[0]
    else:
        return paths[:-1], paths[-1]


def debug():
    pass


def create_google_drive_folder(folder_name: str, parents: list[str]) -> str:
    service = _check_credentials()
    folder_metadata: dict[str, str | list[str]] = {
        'name': folder_name,
        'mimeType': "application/vnd.google-apps.folder",
        'parents': parents
    }
    folder = service.files().create(body=folder_metadata, fields='id,parents').execute()
    print(folder)
    return folder.get('id')


def _folder_exists(folder_name, parent):
    """
    @deprecated method. Use _file_exists() instead
    """
    service = _check_credentials()
    parent_query = ""
    if parent is not None:
        parent_query = f" and '{parent}' in parents"
    page_token = None
    response = service.files().list(
        q=f"mimeType='application/vnd.google-apps.folder' and name = '{folder_name}' and trashed = false{parent_query}",
        spaces="drive",
        fields='nextPageToken, '
               'files(id, name, parents)',
        pageToken=page_token
    ).execute()
    folders = response.get('files', [])
    print(folders)
    if len(folders) == 0:
        return None
    else:
        return folders[-1].get('id')


def delete_from_google_drive(path) -> None:
    try:
        service = _check_credentials()
        folder_path: str | None
        file_name: str
        folder_path, file_name = __split_path(path)
        file_exists: bool = True
        parents: list[str] = []

        if folder_path is not None:
            folder_id: str
            for folder in folder_path:
                if len(parents) != 0:
                    folder_id = _file_exists(folder, parents[0])
                else:
                    folder_id = _file_exists(folder, None)
                if folder_id is None:
                    file_exists = False
                parents = [folder_id]

        file_id: str | None = _file_exists(file_name, parents[0]) \
            if len(parents) != 0 else _file_exists(file_name, None)

        if file_id is None:
            file_exists = False

        if file_exists:
            service.files().delete(fileId=file_id).execute()
        else:
            print("File does not exist")

    except HttpError as error:
        raise HttpError(f"Error occurred: {error}")


def _file_exists(file_name: str, parent: str | None) -> str | None:
    service = _check_credentials()
    mimetype: str = 'application/vnd.google-apps.folder' if '.' not in file_name else EXTENSION_TO_MIMETYPE_MAPPER[
        file_name.split(".")[1]]
    if mimetype in MIMETYPES_TO_BE_EXTENSION_STRIPPED:
        file_name = file_name.split(".")[0]
    parent_query: str = f" and '{parent}' in parents" if parent is not None else " and 'root' in parents"
    page_token = None
    response = service.files().list(
        q=f"mimeType='{mimetype}' and name = '{file_name}' and trashed = false{parent_query}",
        spaces="drive",
        fields='nextPageToken, '
               'files(id, name, parents)',
        pageToken=page_token
    ).execute()
    file = response.get('files', [])
    print(file)
    if len(file) == 0:
        return None
    else:
        return file[-1].get('id')
