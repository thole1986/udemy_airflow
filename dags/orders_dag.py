from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from orders_elt_logic import main


default_args = {
    'owner': 'Tho Le',
    'depends_on_past': False,
    'start_date': datetime(2024, 1 , 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'orders_full_load',
    default_args=default_args,
    description='My Orders Table ETL DAG',
    schedule_interval='@daily'
)

run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=main,
    dag=dag,
)

run_etl
