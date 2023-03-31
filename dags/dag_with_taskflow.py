from datetime import timedelta, datetime

from airflow.decorators import dag, task

default_args = {
    'owner': 'kowal',
    'retry': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id='dag_with_airflow_api',
     default_args=default_args,
     start_date=datetime(2023, 3, 31),
     schedule_interval='@daily'
     )
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': "Jerry",
            'last_name': "Fridman",
        }

    @task()
    def get_age():
        return 18

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello, my name is {first_name} {last_name} and i am {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict["first_name"], last_name=name_dict["last_name"], age=age)


greet_dag = hello_world_etl()
