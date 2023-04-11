from datetime import timedelta, datetime

from airflow.decorators import task, dag
import json

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}


@task()
def extract():
    data = {
        'fiona': 20,
        'grzanka': 16
    }
    return json.dumps(data)


@task()
def transform(raw_data):
    data = json.loads(raw_data)
    heaviest_dog = max(data, key=data.get)
    return heaviest_dog


@task()
def load(data):
    print(data)


@dag(
    dag_id='extract_transform_load_example',
    default_args=default_args,
    catchup=False,
    tags=['ETL'],
    schedule_interval='11 11 1 2 3',
    start_date=datetime(2023, 4, 11),
)
def etl():
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task)


etl_dag = etl()
