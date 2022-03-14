from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from write_to_csv import write_csv
from create_and_insert import create_weather_table
# from task3 import read_and_load_csv

default_args = {
    "owner": "harshita",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("Assignment", default_args=default_args, schedule_interval="0 6 * * *")

# Create the first task to use endpoint /Current Weather Data for at least 10 states of India and fill up the csv file with details of
# State, Description, Temperature, Feels Like Temperature, Min Temperature, Max Temperature, Humidity, Clouds.

t1 = PythonOperator(task_id='Write_into_csv', python_callable=write_csv, dag=dag)

# Create a second task to create a postgres table “Weather” that would have columns same as the csv file.
# Create a third task that should fill the columns of the table while reading the data from the csv file.

t2 = PythonOperator(task_id="create_table", python_callable=create_weather_table, dag=dag)
#
# t3 = PythonOperator(task_id="read_csv_load_data", python_callable=read_and_load_csv, dag=dag)

t1 >> t2
