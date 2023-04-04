from __future__ import print_function

from datetime import timedelta, datetime

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from airflow.decorators import dag, task
from src.google_drive_handler import _check_credentials, send_image_from_disk

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
        send_image_from_disk('kotek.jpg')

    send_file()


t = send_file_to_google_drive()
