from airflow.decorators import dag, task
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from src.google_drive_handler import send_image_from_disk, move_file_to_folder, delete_from_google_drive, \
    download_jpg_file, delete_trash, send_csv_from_disk

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=3)
}


@dag(
    dag_id='example_final_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='37 13 * * *',
    start_date=datetime(2023, 4, 10),
    tags=['google_drive', 'postgres', 'bash']
)
def final_dag():
    @task()
    def send_fiona_image():
        send_image_from_disk("pieski/zdjecia_fiony/fiona.jpg")

    @task()
    def send_grzanka_image():
        send_image_from_disk("pieski/zdjecia_grzanki/grzanka.jpg")

    @task()
    def send_csv():
        send_csv_from_disk("csv1/csv1/test_file.csv")

    @task()
    def move_grzanka_image():
        move_file_to_folder("pieski/zdjecia_grzanki/grzanka.jpg", "pieski/zdjecia_fiony/grzanka.jpg")

    @task()
    def delete_csv():
        delete_from_google_drive("csv1/csv1")

    @task()
    def download_fiona_image():
        return download_jpg_file("pieski/zdjecia_fiony/fiona.jpg", "/downloaded_fiona")

    @task()
    def download_grzanka_image():
        return download_jpg_file("pieski/zdjecia_fiony/grzanka.jpg", "/downloaded_grzanka")

    @task()
    def delete_trash_from_google_drive():
        delete_trash()

    sleep_task = BashOperator(
        task_id='sleep',
        bash_command='sleep 30'
    )

    # @task

    send_fiona_image >> send_grzanka_image
    [send_csv, send_grzanka_image] >> sleep_task
    sleep_task >> [delete_csv, move_grzanka_image]
    move_grzanka_image >> [download_grzanka_image, download_fiona_image]


final_dag()
