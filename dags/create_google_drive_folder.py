from airflow.decorators import task
from airflow import DAG
from datetime import timedelta, datetime
from src.google_drive_handler import create_google_drive_folder, send_csv_from_disk

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='create_google_drive_folder',
        default_args=default_args,
        catchup=False,
        start_date=datetime(2023, 4, 4),
        schedule_interval='@daily',
        tags=['google_drive']
) as dag:
    @task()
    def create_folder():
        return create_google_drive_folder('airflow')


    create_folder()
