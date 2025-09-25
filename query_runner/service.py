# query_runner/service.py
import json, logging, os, time
from typing import Any, Dict, Optional
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dag-launcher")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_Q_REQ  = os.getenv("TOPIC_Q_REQ", "weather.queries.requests")
TOPIC_Q_RES  = os.getenv("TOPIC_Q_RES", "weather.queries.results")

AIRFLOW_BASE    = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")  # SANS /api/vN
AIRFLOW_DAG_ID  = os.getenv("AIRFLOW_DAG_ID", "query_runner")
AIRFLOW_USER    = os.getenv("AIRFLOW_API_USERNAME", "admin")
AIRFLOW_PASS    = os.getenv("AIRFLOW_API_PASSWORD", "admin")
AIRFLOW_TOKEN   = os.getenv("AIRFLOW_API_TOKEN", "").strip()  # optionnel (pré-provisionné)
AIRFLOW_TOKEN_URL = os.getenv("AIRFLOW_TOKEN_URL", f"{AIRFLOW_BASE}/auth/token")

_session = requests.Session()
_bearer: Optional[str] = AIRFLOW_TOKEN or None

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _get_token(force: bool = False) -> str:
    global _bearer
    if _bearer and not force:
        return _bearer
    # password grant style
    payload = {"username": AIRFLOW_USER, "password": AIRFLOW_PASS}
    r = _session.post(AIRFLOW_TOKEN_URL, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()
    _bearer = data.get("access_token")
    if not _bearer:
        raise RuntimeError("No access_token in /auth/token response")
    log.info("Obtained Airflow API token")
    return _bearer

def _post_v2_with_bearer(conf: Dict[str, Any], retry_on_401: bool = True) -> str:
    url = f"{AIRFLOW_BASE}/api/v2/dags/{AIRFLOW_DAG_ID}/dagRuns"
    payload = {"logical_date": _now_iso(), "conf": conf}
    token = _get_token(force=False)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = _session.post(url, json=payload, headers=headers, timeout=20)
    if r.status_code == 401 and retry_on_401:
        log.warning("401 from Airflow API, refreshing token…")
        _get_token(force=True)
        return _post_v2_with_bearer(conf, retry_on_401=False)
    r.raise_for_status()
    return r.json().get("dag_run_id", "unknown")

def main():
    log.info("Starting DAG launcher (Kafka->Airflow v2, Bearer token)…")
    consumer = KafkaConsumer(
        TOPIC_Q_REQ,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=30000,
        group_id="query-runner",
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20, acks=1,
    )

    # warm-up token (optional)
    try:
        _get_token(force=not bool(AIRFLOW_TOKEN))
    except Exception as e:
        log.warning("Token warm-up skipped: %s", e)

    while True:
        try:
            for msg in consumer:
                conf = msg.value or {}
                rid = conf.get("request_id")
                try:
                    dag_run_id = _post_v2_with_bearer(conf)
                    log.info("Triggered DAG: %s (request_id=%s)", dag_run_id, rid)
                    producer.send(TOPIC_Q_RES, value={
                        "request_id": rid, "status": "submitted",
                        "dag_run_id": dag_run_id, "ts": time.time()
                    })
                    producer.flush()
                except Exception as e:
                    log.exception("Failed to trigger DAG")
                    producer.send(TOPIC_Q_RES, value={
                        "request_id": rid, "status": "error",
                        "error": str(e), "ts": time.time()
                    })
                    producer.flush()
        except Exception as e:
            log.exception("Fatal loop error; retry in 3s")
            time.sleep(3)

if __name__ == "__main__":
    main()
