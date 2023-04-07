from datetime import timedelta, datetime

from airflow.decorators import dag, task
from src.google_drive_handler import move_file_to_folder

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.appdata']

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=3)
}


@dag(
    dag_id='move_file_in_google_drive',
    tags=['google_drive'],
    default_args=default_args,
    start_date=datetime(2023, 4, 4),
    catchup=False,
    schedule_interval='@daily'
)
def move_file_dag():
    @task()
    def move():
        return move_file_to_folder('kotek2/kotek.jpg', 'kotek1')

    move()


t = move_file_dag()
