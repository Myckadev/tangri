import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# NOTE: adapte le chemin du script Spark si nécessaire
SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark/batch_daily.py"
HDFS_LS = "hdfs dfs -ls hdfs://namenode:8020/datalake/gold/daily_weather/date=$(date -u +%F) || true"

@dag(
    dag_id="batch_daily",
    description="Daily Spark batch aggregates",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["batch","spark"],
    is_paused_upon_creation=False,
    default_args={"owner":"airflow","retries":0}
)
def batch_daily():
    run_batch = BashOperator(
        task_id="spark_batch_daily",
        bash_command=SPARK_SUBMIT
    )
    check_output = BashOperator(
        task_id="check_today_partition",
        bash_command=HDFS_LS
    )
    run_batch >> check_output

dag = batch_daily()
