import datetime as dt
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

"""
Exercise 2

Create a DAG which will run on your birthday to congratulate you.
"""

MY_NAME = "Pauline"
MY_BIRTHDATE = pendulum.datetime(1990, 8, 1)

dag = DAG(
    dag_id="happy_birthday_v1",
    description="Wishes you a happy birthday",
    default_args={"owner": "Airflow"},
    schedule_interval="0 0 4 8 *",
    start_date=MY_BIRTHDATE,
)

birthday_greeting = BashOperator(
    task_id="send_wishes",
    dag=dag,
    bash_command=f"echo 'Happy birthday, {MY_NAME}!'",
)
