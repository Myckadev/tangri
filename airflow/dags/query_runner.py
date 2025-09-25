# airflow/dags/query_runner.py
import json
import os
import sys
import socket
import traceback
import pendulum
import logging
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)

# -------------------------
# Debug helpers
# -------------------------
def _ts() -> str:
    return pendulum.now("UTC").to_iso8601_string()

def dbg(msg: str) -> None:
    m = f"[{_ts()}][DEBUG] {msg}"
    print(m, flush=True)
    log.info(m)

def err(msg: str) -> None:
    m = f"[{_ts()}][ERROR] {msg}"
    print(m, flush=True)
    log.error(m)

# -------------------------
# Config via env
# -------------------------
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_SCRIPT = os.getenv("SPARK_QUERY_SCRIPT", "/opt/spark_jobs/run_query.py")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_Q_RES  = os.getenv("TOPIC_Q_RES", "weather.queries.results")

DEFAULT_CONF = {
    "request_id": "no-request-id",
    "city_ids": [],
    "date_from": "",
    "date_to": "",
    "metrics": ["temperature_2m"],
    "agg": "avg",
}

def _merge_conf(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    c = dict(base)
    if override:
        c.update({k: v for k, v in override.items() if v is not None})
    return c

# -------------------------
# DAG
# -------------------------
@dag(
    dag_id="query_runner",
    description="Parametric weather query -> Spark -> HDFS -> status to Kafka",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["queries","spark","debug"],
)
def query_runner():

    @task
    def debug_env_and_context():
        try:
            ctx = get_current_context()
            dbg("ENTER debug_env_and_context()")
            dbg(f"hostname={socket.gethostname()} python={sys.version.split()[0]}")
            dbg(f"env.SPARK_MASTER_URL={SPARK_MASTER}")
            dbg(f"env.SPARK_QUERY_SCRIPT={SPARK_SCRIPT}")
            dbg(f"env.KAFKA_BROKER={KAFKA_BROKER} env.TOPIC_Q_RES={TOPIC_Q_RES}")
            # Env Airflow utiles
            for k in [
                "AIRFLOW__CORE__EXECUTOR",
                "AIRFLOW__LOGGING__LOGGING_LEVEL",
                "AIRFLOW__CORE__DAGS_FOLDER",
                "AIRFLOW__CORE__HOSTNAME_CALLABLE",
            ]:
                dbg(f"env.{k}={os.getenv(k, '')}")
            # Contexte
            keys = sorted(list(ctx.keys()))
            dbg(f"context.keys={keys}")
            dag_run = ctx.get("dag_run")
            dbg(f"dag_run={dag_run!r}")
            raw_conf = getattr(dag_run, "conf", {}) or {}
            dbg(f"dag_run.conf(raw)={json.dumps(raw_conf)}")
            return True
        except Exception:
            err("debug_env_and_context failed:\n" + traceback.format_exc())
            # On remonte l’exception pour marquer la task en failed (afin de voir les logs).
            raise

    @task
    def get_conf() -> Dict[str, Any]:
        dbg("ENTER get_conf()")
        try:
            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            dag_run_conf = getattr(dag_run, "conf", {}) or {}
            dbg(f"[get_conf] raw dag_run.conf: {json.dumps(dag_run_conf)}")
            conf = _merge_conf(DEFAULT_CONF, dag_run_conf)
            # normaliser
            conf["city_ids"] = list(conf.get("city_ids") or [])
            conf["metrics"]  = list(conf.get("metrics") or ["temperature_2m"])
            conf["agg"]      = (conf.get("agg") or "avg").lower()
            dbg(f"[get_conf] effective conf: {json.dumps(conf)}")
            return conf
        except Exception:
            err("[get_conf] ERROR:\n" + traceback.format_exc())
            raise

    # Instantiate tasks
    debug = debug_env_and_context()
    conf = get_conf()

    # La commande spark : on affiche *tout* (set -x) et on echo la commande finale.
    spark_cmd = BashOperator(
        task_id="spark_run_query",
        bash_command=(
            "set -euxo pipefail; "
            "echo '[spark_run_query] $(date -u +%FT%TZ) hostname='$(hostname) ; "
            "echo '[spark_run_query] SPARK_MASTER_URL' '" + SPARK_MASTER + "' ; "
            "echo '[spark_run_query] SPARK_QUERY_SCRIPT' '" + SPARK_SCRIPT + "' ; "
            "REQ_ID='{{ ti.xcom_pull(task_ids=\"get_conf\")[\"request_id\"] }}'; "
            "CITY_IDS='{{ (ti.xcom_pull(task_ids=\"get_conf\")[\"city_ids\"] or []) | join(\",\") }}'; "
            "DATE_FROM='{{ ti.xcom_pull(task_ids=\"get_conf\")[\"date_from\"] or \"\" }}'; "
            "DATE_TO='{{ ti.xcom_pull(task_ids=\"get_conf\")[\"date_to\"] or \"\" }}'; "
            "METRICS='{{ (ti.xcom_pull(task_ids=\"get_conf\")[\"metrics\"] or []) | join(\",\") }}'; "
            "AGG='{{ ti.xcom_pull(task_ids=\"get_conf\")[\"agg\"] }}'; "
            "echo '[spark_run_query] params: req_id='$REQ_ID', city_ids='$CITY_IDS', date_from='$DATE_FROM', date_to='$DATE_TO', metrics='$METRICS', agg='$AGG'; "
            "CMD=(spark-submit --master " + SPARK_MASTER + " --conf spark.jars.ivy=/tmp/.ivy2 " + SPARK_SCRIPT + " "
            "--request_id \"$REQ_ID\" --city_ids \"$CITY_IDS\" --date_from \"$DATE_FROM\" --date_to \"$DATE_TO\" --metrics \"$METRICS\" --agg \"$AGG\"); "
            "printf '[spark_run_query] CMD:'; printf ' %q' \"${CMD[@]}\"; echo; "
            "\"${CMD[@]}\" "
        ),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def publish_status_final():
        dbg("ENTER publish_status_final()")
        try:
            ctx = get_current_context()
            dag = ctx["dag"]
            dag_run = ctx["dag_run"]
            ti_self = ctx["ti"]
            dbg(f"[publish] dag_run_id={dag_run.run_id} dag_id={dag_run.dag_id}")

            # Conf effective
            conf_val = ti_self.xcom_pull(task_ids="get_conf", key="return_value")
            if not conf_val:
                conf_val = _merge_conf(DEFAULT_CONF, getattr(dag_run, "conf", {}) or {})
                dbg("[publish] conf came from dag_run.conf (xcom missing)")
            else:
                dbg("[publish] conf came from XCom")
            dbg(f"[publish] conf: {json.dumps(conf_val)}")

            rid = conf_val.get("request_id", "no-request-id")

            # Lire états des upstream
            upstream_ids: List[str] = sorted(dag.get_task("publish_status_final").upstream_task_ids)
            dbg(f"[publish] upstream_ids={upstream_ids}")

            try:
                with create_session() as session:
                    tis: List[TaskInstance] = (
                        session.query(TaskInstance)
                        .filter(
                            TaskInstance.dag_id == dag_run.dag_id,
                            TaskInstance.run_id == dag_run.run_id,
                            TaskInstance.task_id.in_(upstream_ids),
                        )
                        .all()
                    )
                dbg(f"[publish] fetched {len(tis)} upstream TaskInstances")
                if not tis:
                    dag_state = str(getattr(dag_run, "state", "") or "")
                    overall_ok = dag_state.lower() == "success"
                    upstream_map = {"dag_run_state": str(dag_run.state)}
                else:
                    overall_ok = all(ti.state == TaskInstanceState.SUCCESS for ti in tis)
                    upstream_map = {ti.task_id: (ti.state or "unknown") for ti in tis}
            except Exception:
                err("[publish] ERROR while reading upstream TIs:\n" + traceback.format_exc())
                overall_ok = False
                upstream_map = {"error": "upstream-read-failed"}

            status = "success" if overall_ok else "error"

            payload = {
                "request_id": rid,
                "status": status,
                "result_path": f"hdfs://namenode:8020/datalake/gold/queries/{rid}",
                "upstream": upstream_map,
                "ts": pendulum.now("UTC").to_iso8601_string(),
            }
            dbg(f"[publish] prepared payload: {json.dumps(payload)}")

            # Envoi Kafka – ne jamais faire planter la task
            try:
                dbg(f"[publish] importing kafka client…")
                from kafka import KafkaProducer  # import ici pour voir si ça casse
                dbg(f"[publish] creating producer to {KAFKA_BROKER}")
                p = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    linger_ms=20, acks=1,
                )
                dbg(f"[publish] sending to topic {TOPIC_Q_RES}")
                p.send(TOPIC_Q_RES, value=payload)
                p.flush()
                dbg("[publish] Kafka send OK")
            except Exception:
                err("[publish] ERROR sending to Kafka:\n" + traceback.format_exc())
            return payload
        except Exception:
            err("[publish] FATAL:\n" + traceback.format_exc())
            raise

    # Dépendances
    debug >> conf >> spark_cmd >> publish_status_final()

dag = query_runner()
