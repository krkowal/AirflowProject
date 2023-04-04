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
        dag_id='send_image_to_google_drive',
        default_args=default_args,
        catchup=False,
        start_date=datetime(2023, 4, 4),
        schedule_interval='@daily',
        tags=['google_drive']
) as dag:
    @task()
    def send_csv():
        send_csv_from_disk('test_file.csv')
    

    send_csv()
