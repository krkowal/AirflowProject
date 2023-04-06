from __future__ import print_function

from datetime import timedelta, datetime

from airflow.decorators import dag, task
from src.google_drive_handler import delete_from_google_drive

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=3)
}


@dag(
    dag_id='delete_file_from_google_drive',
    tags=['google_drive'],
    default_args=default_args,
    start_date=datetime(2023, 4, 4),
    catchup=False,
    schedule_interval='@daily'
)
def delete_file_google_drive():
    @task()
    def delete_file():
        delete_from_google_drive('kotek.jpg')

    delete_file()


t = delete_file_google_drive()
