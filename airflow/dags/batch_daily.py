import logging
import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

@dag(
    dag_id="batch_daily",
    description="hello batch DAG (Airflow 3.x)",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["hello", "batch"],
    default_args={"owner": "hello", "retries": 0},
)
def batch_daily():
    @task(task_id="hello_task")
    def hello_batch():
        log.info("hello from Airflow DAG batch_daily")

    hello_batch()

dag = batch_daily()
