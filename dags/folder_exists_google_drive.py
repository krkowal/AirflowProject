from airflow.decorators import task
from airflow import DAG
from datetime import timedelta, datetime
from src.google_drive_handler import _folder_exists

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='check_if_folder_exists',
        default_args=default_args,
        catchup=False,
        start_date=datetime(2023, 4, 4),
        schedule_interval='@daily',
        tags=['google_drive']
) as dag:
    @task()
    def check_if_folder_exists():
        return _folder_exists('airflow')


    check_if_folder_exists()
