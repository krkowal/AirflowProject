from airflow.decorators import task
from airflow import DAG
from datetime import timedelta, datetime
from src.google_drive_handler import send_csv_from_disk

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='send_csv_to_google_drive',
        default_args=default_args,
        catchup=False,
        start_date=datetime(2023, 4, 4),
        schedule_interval='@daily',
        tags=['google_drive']
) as dag:
    @task()
    def send_csv(path):
        send_csv_from_disk(path)


    @task()
    def second_send_csv(path):
        send_csv_from_disk(path)


    send_csv('csv1/csv2/csv1/csv2/test_file.csv') >> second_send_csv('csv1/csv2/csv1/csv2/test2.csv')
