import json
import logging
import os
import subprocess
import sys
import time
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("query_runner")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_Q_REQ = os.getenv("TOPIC_Q_REQ", "weather.queries.requests")
TOPIC_Q_RES = os.getenv("TOPIC_Q_RES", "weather.queries.results")
HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://namenode:8020/datalake")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_SCRIPT = os.getenv("SPARK_SCRIPT", "/opt/spark_jobs/run_query.py")


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_Q_REQ,
        bootstrap_servers=[KAFKA_BROKER],
        group_id="query-runner",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        consumer_timeout_ms=0,
    )


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
        retries=3,
    )


def submit_spark(request: Dict[str, Any]) -> Dict[str, Any]:
    request_id = request["request_id"]
    query = request.get("query", {})
    city_ids = ",".join(query.get("city_ids", []))
    date_from = query.get("date_from")
    date_to = query.get("date_to")
    metrics = ",".join(query.get("metrics", ["temperature_2m"]))
    agg = query.get("agg") or "avg"

    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.hadoop.fs.defaultFS=hdfs://namenode:8020",
        "--conf", "spark.hadoop.hadoop.security.authentication=simple",
        "--conf", "spark.driver.extraJavaOptions=-Duser.name=root",
        "--conf", "spark.executor.extraJavaOptions=-Duser.name=root",
        SPARK_SCRIPT,
        "--request_id", request_id,
        "--city_ids", city_ids,
        "--date_from", date_from or "",
        "--date_to", date_to or "",
        "--metrics", metrics,
        "--agg", agg,
    ]

    env = os.environ.copy()
    env["USER"] = "root"
    env["HADOOP_USER_NAME"] = "root"
    logger.info("Submitting Spark job: %s", " ".join(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
    stdout_lines = []
    assert proc.stdout is not None
    for line in proc.stdout:
        stdout_lines.append(line.rstrip())
        # Try to parse JSON status if present
        try:
            if line.lstrip().startswith("{"):
                obj = json.loads(line)
                if obj.get("request_id") == request_id:
                    status = obj
        except Exception:
            pass
        sys.stdout.write(line)
        sys.stdout.flush()
    code = proc.wait()

    # Fallback status if not parsed
    status_obj: Dict[str, Any] = {
        "request_id": request_id,
        "status": "done" if code == 0 else "failed",
        "result_path": f"{HDFS_BASE}/gold/queries/{request_id}",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "exit_code": code,
        "logs_tail": stdout_lines[-20:],
    }

    # If spark printed a JSON status, prefer it
    try:
        if 'status' in locals() and isinstance(status, dict):
            status_obj.update(status)
    except Exception:
        pass

    return status_obj


def main() -> None:
    logger.info("Starting query_runner. broker=%s req_topic=%s res_topic=%s", KAFKA_BROKER, TOPIC_Q_REQ, TOPIC_Q_RES)
    consumer = create_consumer()
    producer = create_producer()

    for msg in consumer:
        try:
            payload = msg.value
            request_id = payload.get("request_id")
            logger.info("Received request_id=%s", request_id)
            result = submit_spark(payload)
            logger.info("Publishing result for request_id=%s", request_id)
            producer.send(TOPIC_Q_RES, value=result)
            producer.flush()
        except Exception as exc:
            logger.exception("Failed processing message: %s", exc)


if __name__ == "__main__":
    main() 