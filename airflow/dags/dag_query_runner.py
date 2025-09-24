from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_query_runner():
    print("hello from Airflow DAG query_runner")

default_args = {"owner": "hello"}

with DAG(
    dag_id="query_runner",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # déclenché à la demande
    catchup=False,
    description="hello query runner DAG",
) as dag:
    t1 = PythonOperator(
        task_id="hello_query",
        python_callable=hello_query_runner
    )
