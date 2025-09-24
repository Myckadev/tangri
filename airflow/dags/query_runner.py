import logging
import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

@dag(
    dag_id="query_runner",
    description="Hello query runner DAG (triggered on demand)",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["hello", "queries"],
    default_args={"owner": "hello"},
)
def query_runner():
    @task(task_id="hello_query")
    def hello_query_runner():
        log.info("hello from Airflow DAG query_runner")

    hello_query_runner()

dag = query_runner()
