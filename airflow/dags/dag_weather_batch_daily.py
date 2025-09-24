from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_batch():
    print("hello from Airflow DAG batch_daily")

default_args = {
    "owner": "hello",
    "retries": 0,
}

with DAG(
    dag_id="batch_daily",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="hello batch DAG",
) as dag:
    t1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello_batch
    )
