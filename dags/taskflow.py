from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime


def _task_a():
    print("Task A")
    return 42


@dag(
    # dag_id='taskflow',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['taskflow']
)

def taskflow():

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=_task_a
    )

    @task
    def task_b(value):
        print("Task B")
        print(value)

    # this: xcom_pull(task_id='task_a') the same as b
    task_b(task_a.output)

taskflow()
