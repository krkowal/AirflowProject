# Instalation

1. Check docker compose version

`docker --version` must be v1.29+

2. Create docker container

`docker compose up airflow-init`

3. Set up venv -> add python interpreter -> new venv
4. Install airflow package

- pycharm: python packages -> search apache-airflow -> install
- or `pip install apache-airflow`

4. Install apache-airflow-providers-postgres package using pip or pycharm packages

- <span style="color:red">If it does not show up - restart IDE</span>.

# Running

`Docker compose up -d`

Airflow web ui - http://localhost:8080/

credentials:

- login: airflow
- password: airflow

# Database

1. Postgres:

- username: airflow
- password: airflow
- port: 5432


