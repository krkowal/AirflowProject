from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'oski',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(ti):
    param1 = ti.xcom_pull(task_ids='get_params', key='param1')
    param2 = ti.xcom_pull(task_ids='get_params', key='param2')
    param3 = ti.xcom_pull(task_ids='get_new_param', key='param3')
    print(f"Hello task - {param1} - {param2} - {param3}")


def get_params(ti):
    ti.xcom_push(key='param1', value='param_X')
    ti.xcom_push(key='param2', value='param_Y')


def get_new_param(ti):
    ti.xcom_push(key='param3', value='param_Z')


with DAG(
        default_args=default_args,
        dag_id='python_operators_example',
        description='first dag with python operator',
        start_date=datetime(2023, 3, 30),
        schedule_interval='@daily',
        tags=["python"]
) as dag:
    task1 = PythonOperator(
        task_id='greeting',
        python_callable=greet,
        # op_kwargs={
        #     'param1': 'param1',
        # }
    )

    task2 = PythonOperator(
        task_id='get_params',
        python_callable=get_params,
    )

    task3 = PythonOperator(
        task_id='get_new_param',
        python_callable=get_new_param
    )

    [task2, task3] >> task1

    task2
