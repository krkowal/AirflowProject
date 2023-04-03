from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
        dag_id="demo_dag",
        start_date=datetime(2023, 3, 28),
        schedule="17 17 * * *",
        tags=["python"]
) as dag:
    @task()
    def test_airflow():
        print("Executed using Apache Airflow ✨")


    test_airflow()
