from __future__ import annotations

import io
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SCOPES: list[str] = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
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

"""
Some files with google mimetypes are stripped of their extension when saving to Google Drive, so in order to find them
it is needed to strip files of their extension.
"""
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


SERVICE = _check_credentials()


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
        service = SERVICE
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
    service = SERVICE
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
    service = SERVICE
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


def delete_from_google_drive(path: str) -> None:
    try:
        service = SERVICE
        file_id: str = _path_exists(path)
        if file_id is not None:
            service.files().delete(fileId=file_id).execute()
        else:
            print("File does not exist")

    except HttpError as error:
        raise HttpError(f"Error occurred: {error}")


def _path_exists(path: str) -> str | None:
    try:
        folder_path: str | None
        file_name: str
        folder_path, file_name = __split_path(path)
        file_exists: bool = True
        parents: list[str] = []
        print(file_exists)
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
        print(file_exists)
        print(parents)
        if not file_exists:
            return None
        file_id: str | None = _file_exists(file_name, parents[0]) \
            if len(parents) != 0 else _file_exists(file_name, None)

        if file_id is not None:
            print(f"file exists: {file_id}")
            return file_id

        return None
    except HttpError as error:
        print(f"error occured: {error}")


def _file_exists(file_name: str, parent: str | None) -> str | None:
    try:
        service = SERVICE
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
    except HttpError as error:
        print(f"error occurred: {error}")


def move_file_to_folder(source_path: str, target_path: str) -> bool:
    file_name: str = source_path.split("/")[-1]
    target_name: str = target_path.split("/")[-1]
    try:
        service = SERVICE

        if "." not in file_name:
            print("Cannot move folders")
            return False

        if "." in target_name:
            print("Cannot move files into other files")
            return False

        source_id: str | None = _path_exists(source_path)
        if source_id is None:
            print("Source file does not exist!")
            return False

        target_id: str = _path_exists(target_path)
        if target_id is None:
            print("Target folder does not exist!")
            return False

        if _path_exists(f"{target_path}/{file_name}") is not None:
            print("File with that name already exists in target folder!")
            return False

        parent_id = service.files().get(fileId=source_id, fields='parents').execute()["parents"][0]
        service.files().update(fileId=source_id, removeParents=parent_id, addParents=target_id,
                               fields='id,parents').execute()
        return True

    except HttpError as error:
        print(f"error occured: {error}")


def delete_trash() -> None:
    try:
        service = SERVICE
        service.files().emptyTrash().execute()
    except HttpError as error:
        print(f"error occurred: {error}")


def __list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))


def _download_file_from_google_drive(path: str, path_to_store: str = "", func=None) -> bool:
    """
    Downloads file from Google Drive to local airflow folders -> local (project) folders
    (because local folders are mounted in airflow folders)

    Parameters:
    path: str - path to google drive file e.g. folder1/folder2/file.extension
    path_to_store: str | default "" - path in which file will be stored locally.
                                      MUST START WITH '/'! e.g. /folder1/folder2
                                      Target folder must be created beforehand
    func - request function that depends on file type in Google Drive.
           files:get_media (default) must be used when downloading not Google files e.g. jpg or plain csv file.
           files:export must be used when downloading e.g. spreadsheets
    """
    try:
        service = SERVICE
        file_name = path.split("/")[-1]
        if "." not in file_name:
            print("Cannot download folder")
            return False
        file_id = _path_exists(path)
        if file_id is None:
            print("File does not exist!")
            return False

        request = service.files().get_media(fileId=file_id) if func is None else func(file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fd=fh, request=request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(F'Download {int(status.progress() * 100)}.')
        fh.seek(0)
        with open(
                os.path.join(
                    f'/opt/airflow/{EXTENSION_TO_AIRFLOW_FOLDER_MAPPER[file_name.split(".")[1]]}{path_to_store}',
                    file_name),
                'wb') as f:
            f.write(fh.read())
            f.close()
        __list_files("/opt/airflow")
        return True
    except HttpError as error:
        print(f"error occurred: {error}")


def download_csv_from_spreadsheet(path: str, path_to_store: str = "") -> bool:
    service = SERVICE

    def download(file_id):
        return service.files().export(fileId=file_id, mimeType='text/csv')

    return _download_file_from_google_drive(path, path_to_store, download)


def download_jpg_file(path: str, path_to_store: str) -> bool:
    return _download_file_from_google_drive(path, path_to_store)
