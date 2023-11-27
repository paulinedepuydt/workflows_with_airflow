import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import cross_downstream

"""
Exercise 3

This DAG contains a lot of repetitive, duplicated and ultimately boring code.
Can you simplify this DAG and make it more concise?
"""

dag = DAG(
    dag_id="repetitive_tasks",
    description="Many tasks in parallel",
    default_args={"owner": "Airflow"},
    schedule_interval="@daily",
    start_date=dt.datetime(2021, 1, 1),
    end_date=dt.datetime(2021, 1, 15),
)

def gen_task(id):
    task = BashOperator(
        task_id=f"task_{id}", dag=dag, bash_command=f"echo 'task_{id} done'"
    )
    return task


task_a = gen_task("a")
task_b = gen_task("b")
task_c = gen_task("c")
task_d = gen_task("d")
task_e = gen_task("e")
task_f = gen_task("f")

cross_downstream(from_tasks=[task_a, task_b, task_c], to_tasks=[task_d, task_e, task_f])
