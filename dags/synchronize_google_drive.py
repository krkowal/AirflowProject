from airflow.decorators import task, dag

from datetime import timedelta, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import DataFrame
from src.google_drive_handler import delete_from_google_drive, send_csv_from_disk

default_args = {
    'owner': "kowal",
    'retry': 3,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='synchronize_google_drive',
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily',
    tags=['postgres', 'google_drive'],
    start_date=datetime(2023, 4, 15)
)
def synchronize_google_drive():
    @task()
    def copy():
        pg_hook: PostgresHook = PostgresHook('postgres')
        df: DataFrame = pg_hook.get_pandas_df("select * from nazwy")
        df.to_csv('/opt/airflow/csv/nazwy.csv', index=False)

    @task()
    def synchronize_with_google_drive(path: str):
        delete_from_google_drive(path)
        send_csv_from_disk(path)

    copy() >> synchronize_with_google_drive('synchronizations/nazwy.csv')


s = synchronize_google_drive()
