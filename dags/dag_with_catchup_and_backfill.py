from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'kowal',
    'retry': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        dag_id='dag_with_catchup_and_backfillv2',
        default_args=default_args,
        start_date=datetime(2023, 3, 20),
        schedule_interval='@daily',
        catchup=False,
        tags=["bash"]
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is bash command'
    )
