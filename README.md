# Instalation

1. Check docker compose and python version
   `docker --version` must be v1.29+
   `python --version` must be 3.10.7+
2. Create docker container
   `docker compose up airflow-init`
3. Set up venv -> add python interpreter -> new venv
4. Create folders in root: auth, logs
5. Use requirements.txt to install all packages:

- `pip install -r requirements.txt`
  Or follow steps 5 and 6

7. Install airflow package

- pycharm: python packages -> search apache-airflow -> install
- or `pip install apache-airflow`

8. Install apache-airflow-providers-postgres package using pip or pycharm packages

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

# Google drive api

https://developers.google.com/drive/api/quickstart/python?hl=pl
