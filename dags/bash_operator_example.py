from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'kowal',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id='Bash_operators_example',
         default_args=default_args,
         description='Bash example',
         start_date=datetime(2023, 3, 30),
         schedule_interval='@daily') \
        as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo This is the first task"
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo Task2 which is after task1 "
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo Task3 running after task1"
    )

    # 2 Methods of setting downstream: method .set_downstream and >>
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    # task1 >> task2
    # task1 >> task3

    # we can add multiple downstreams at one time
    task1 >> [task2, task3]

    task1
