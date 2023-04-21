from airflow.decorators import task, dag

from datetime import timedelta, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from src.google_drive_handler import download_csv_from_spreadsheet

default_args = {
    'owner': "kowal",
    'retry': 3,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='synchronize_postgres',
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily',
    tags=['postgres', 'google_drive'],
    start_date=datetime(2023, 4, 15)
)
def synchronize_postgres():
    @task()
    def copy(path: str, path_to_store: str):
        download_csv_from_spreadsheet(path, path_to_store)

    @task()
    def synchronize_with_postgres():
        pg_hook: PostgresHook = PostgresHook('postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        df: pd.DataFrame = pd.read_csv('/opt/airflow/csv/to_postgres/nazwy.csv')
        df.to_sql("kopia_nazwy", engine, if_exists='replace', index=False)

    copy('synchronizations/nazwy.csv', '/to_postgres') >> synchronize_with_postgres()


s = synchronize_postgres()
