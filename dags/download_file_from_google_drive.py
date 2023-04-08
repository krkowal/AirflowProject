from datetime import timedelta, datetime

from airflow.decorators import dag, task
from src.google_drive_handler import download_jpg_file, download_csv_from_spreadsheet

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=3)
}


@dag(
    dag_id='download_file_from_google_drive',
    tags=['google_drive'],
    default_args=default_args,
    start_date=datetime(2023, 4, 4),
    catchup=False,
    schedule_interval='@daily'
)
def download_file_dag():
    @task()
    def download_jpg():
        return download_jpg_file('kotek.jpg', '/test')

    @task()
    def download_csv():
        return download_csv_from_spreadsheet('csv1/test_file.csv', '/t1')

    download_jpg()
    download_csv()


t = download_file_dag()
