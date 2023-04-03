from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'kowal',
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='dag_with_postgresv2',
        default_args=default_args,
        start_date=datetime(2023, 4, 2),
        catchup=False,
        schedule_interval='@daily',
        tags=["postgres"]
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres',
        sql="""
        create table if not exists dag_runs(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
            )
        """
    )
    task1
