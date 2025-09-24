# =========================
# Open-Meteo Lambda — Make (fusion A ∪ B)
# =========================

SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
.ONESHELL:

# Charger .env si présent
ifneq (,$(wildcard .env))
include .env
export $(shell sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .env)
endif

# Valeurs par défaut si non définies
TZ ?= Europe/Paris
HOST ?= localhost

KAFKA_BROKER   ?= kafka:9092
TOPIC_RAW      ?= weather.raw.openmeteo
TOPIC_HOURLY   ?= weather.hourly.flattened
TOPIC_CURRENT  ?= weather.current.metrics
TOPIC_Q_REQ    ?= weather.queries.requests
TOPIC_Q_RES    ?= weather.queries.results

PORT_GRAFANA    ?= 3000
PORT_AIRFLOW    ?= 8088
PORT_SPARK      ?= 8080
PORT_STREAMLIT  ?= 8501
PORT_PROM       ?= 9090
PORT_KAFKAUI    ?= 8081
PORT_HDFS_NN    ?= 9870
PORT_HDFS_DN    ?= 9864

KAFKA_TOPICS := /opt/bitnami/kafka/bin/kafka-topics.sh

# Helpers liens cliquables (OSC-8)
define link
printf "\033]8;;$(1)\033\\$(2)\033]8;;\033\\"
endef

OPEN_CMD := xdg-open
ifeq ($(shell uname),Darwin)
OPEN_CMD := open
endif

URL_GRAFANA    := http://$(HOST):$(PORT_GRAFANA)/
URL_AIRFLOW    := http://$(HOST):$(PORT_AIRFLOW)/
URL_SPARK      := http://$(HOST):$(PORT_SPARK)/
URL_STREAMLIT  := http://$(HOST):$(PORT_STREAMLIT)/
URL_PROM       := http://$(HOST):$(PORT_PROM)/
URL_KAFKAUI    := http://$(HOST):$(PORT_KAFKAUI)/
URL_HDFS_NN    := http://$(HOST):$(PORT_HDFS_NN)/
URL_HDFS_DN    := http://$(HOST):$(PORT_HDFS_DN)/

.PHONY: run up wait-kafka wait-airflow seed-topics spark-streaming spark-streaming-localfs spark-streaming-hdfs \
        airflow-batch stop down logs show-infra status urls \
        open-grafana open-airflow open-spark open-streamlit open-prom open-kafkaui open-hdfs-nn open-hdfs-dn \
        check-env clean-all nuke

# --------
# Orchestrations
# --------
run:
	@echo "-> Démarrage de la stack..."
	$(MAKE) up
	@echo "-> Attente de Kafka..."
	$(MAKE) wait-kafka
	@echo "-> Création des topics..."
	$(MAKE) seed-topics
	@echo "-> Lancement du streaming Spark..."
	$(MAKE) spark-streaming
	@echo "-> Attente d'Airflow..."
	$(MAKE) wait-airflow
	@echo "-> Déclenchement du batch via Airflow..."
	$(MAKE) airflow-batch
	@echo ""
	$(MAKE) status

up:
	@echo "-> docker compose up -d --build --force-recreate"
	@docker compose up -d --build --force-recreate

# --------
# Checks readiness
# --------
wait-kafka:
	@retries=30; \
	while [ $$retries -gt 0 ]; do \
	  if docker compose exec -T kafka bash -lc "$(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BROKER) --list >/dev/null 2>&1"; then \
	    echo "-> Kafka est prêt."; exit 0; \
	  else \
	    echo "-> Kafka pas encore prêt, nouvelle tentative"; \
	    sleep 2; \
	    retries=$$((retries-1)); \
	  fi; \
	done; \
	echo "-> Kafka n'a pas démarré à temps." && exit 1

# Airflow 3.x: health via API v2
wait-airflow:
	@retries=40; \
	while [ $$retries -gt 0 ]; do \
	  if docker compose exec -T airflow-webserver bash -lc "curl -fsS http://localhost:8080/api/v2/monitor/health | grep -q '\"status\":\"healthy\"'"; then \
	    echo "-> Airflow est prêt."; exit 0; \
	  else \
	    echo "-> Airflow pas encore prêt, nouvelle tentative"; \
	    sleep 3; \
	    retries=$$((retries-1)); \
	  fi; \
	done; \
	echo "-> Airflow n'a pas démarré à temps." && exit 1

# --------
# Kafka topics
# --------
seed-topics:
	@echo "-> Vérif variables:"
	@echo "   RAW=$(TOPIC_RAW) | HOURLY=$(TOPIC_HOURLY) | CURRENT=$(TOPIC_CURRENT)"
	@echo "   Q_REQ=$(TOPIC_Q_REQ) | Q_RES=$(TOPIC_Q_RES) | BROKER=$(KAFKA_BROKER)"
	@docker compose exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists --topic "$(TOPIC_RAW)"     --bootstrap-server "$(KAFKA_BROKER)" --partitions 6 --replication-factor 1
	@docker compose exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists --topic "$(TOPIC_HOURLY)"  --bootstrap-server "$(KAFKA_BROKER)" --partitions 6 --replication-factor 1
	@docker compose exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists --topic "$(TOPIC_CURRENT)" --bootstrap-server "$(KAFKA_BROKER)" --partitions 6 --replication-factor 1
	@docker compose exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists --topic "$(TOPIC_Q_REQ)"   --bootstrap-server "$(KAFKA_BROKER)" --partitions 3 --replication-factor 1
	@docker compose exec -T kafka $(KAFKA_TOPICS) --create --if-not-exists --topic "$(TOPIC_Q_RES)"   --bootstrap-server "$(KAFKA_BROKER)" --partitions 3 --replication-factor 1
	@echo "-> Topics créés (ou déjà existants)."

# --------
# Spark (par défaut: localfs pour éviter les soucis UGI/HDFS)
# --------
spark-streaming: spark-streaming-localfs

spark-streaming-localfs:
	@echo "-> spark-submit (file://)"
	@docker compose exec -T spark-master bash -lc '\
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.jars.ivy=/tmp/.ivy2 \
	    --conf spark.hadoop.fs.defaultFS=file:/// \
	    --conf spark.hadoop.hadoop.security.authentication=simple \
	    --conf spark.driver.extraJavaOptions="-Duser.name=$$USER" \
	    --conf spark.executor.extraJavaOptions="-Duser.name=$$USER" \
	    --conf spark.executorEnv.USER=$$USER \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	    /opt/spark_jobs/streaming_hourly.py || true'

# Variante HDFS: si/quant tu veux écrire/lire depuis HDFS (namenode)
spark-streaming-hdfs:
	@echo "-> spark-submit (hdfs://namenode:8020)"
	@docker compose exec -T -e USER=root -e HADOOP_USER_NAME=root spark-master bash -lc '\
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.jars.ivy=/tmp/.ivy2 \
	    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
	    --conf spark.hadoop.hadoop.security.authentication=simple \
	    --conf spark.driver.extraJavaOptions="-Duser.name=$$USER" \
	    --conf spark.executor.extraJavaOptions="-Duser.name=$$USER" \
	    --conf spark.executorEnv.USER=$$USER \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	    /opt/spark_jobs/streaming_hourly.py || true'

# --------
# Airflow (déclenchement DAG)
# --------
airflow-batch:
	@echo "-> Déclenchement manuel d'un DAG batch dans Airflow"
	@docker compose exec -T airflow-webserver airflow dags trigger batch_daily || true

# --------
# Infra utils
# --------
stop:
	@echo "-> docker compose stop"
	@docker compose stop

down:
	@echo "-> docker compose down -v"
	@docker compose down -v

logs:
	@docker compose logs -f --tail=200

show-infra:
	@watch -n 0.5 docker ps -a

status:
	@echo "-> Conteneurs:" && docker compose ps
	@echo "\n-> Topics Kafka:" && docker compose exec -T kafka $(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BROKER) --list || true
	@echo "\n-> UIs:"
	@printf "   Grafana     -> "; $(call link,$(URL_GRAFANA),$(URL_GRAFANA)); echo
	@printf "   Airflow     -> "; $(call link,$(URL_AIRFLOW),$(URL_AIRFLOW)); echo
	@printf "   Spark UI    -> "; $(call link,$(URL_SPARK),$(URL_SPARK)); echo
	@printf "   NameNode    -> "; $(call link,$(URL_HDFS_NN),$(URL_HDFS_NN)); echo
	@printf "   DataNode    -> "; $(call link,$(URL_HDFS_DN),$(URL_HDFS_DN)); echo
	@printf "   Streamlit   -> "; $(call link,$(URL_STREAMLIT),$(URL_STREAMLIT)); echo
	@printf "   Prometheus  -> "; $(call link,$(URL_PROM),$(URL_PROM)); echo
	@printf "   Kafka UI    -> "; $(call link,$(URL_KAFKAUI),$(URL_KAFKAUI)); echo

urls:
	@echo "-> UIs:"
	@printf "   Grafana     -> "; $(call link,$(URL_GRAFANA),$(URL_GRAFANA)); echo
	@printf "   Airflow     -> "; $(call link,$(URL_AIRFLOW),$(URL_AIRFLOW)); echo
	@printf "   Spark UI    -> "; $(call link,$(URL_SPARK),$(URL_SPARK)); echo
	@printf "   NameNode    -> "; $(call link,$(URL_HDFS_NN),$(URL_HDFS_NN)); echo
	@printf "   DataNode    -> "; $(call link,$(URL_HDFS_DN),$(URL_HDFS_DN)); echo
	@printf "   Streamlit   -> "; $(call link,$(URL_STREAMLIT),$(URL_STREAMLIT)); echo
	@printf "   Prometheus  -> "; $(call link,$(URL_PROM),$(URL_PROM)); echo
	@printf "   Kafka UI    -> "; $(call link,$(URL_KAFKAUI),$(URL_KAFKAUI)); echo

open-grafana:
	@$(OPEN_CMD) "$(URL_GRAFANA)" >/dev/null 2>&1 & echo "-> $(URL_GRAFANA)"

open-airflow:
	@$(OPEN_CMD) "$(URL_AIRFLOW)" >/dev/null 2>&1 & echo "-> $(URL_AIRFLOW)"

open-spark:
	@$(OPEN_CMD) "$(URL_SPARK)" >/dev/null 2>&1 & echo "-> $(URL_SPARK)"

open-streamlit:
	@$(OPEN_CMD) "$(URL_STREAMLIT)" >/dev/null 2>&1 & echo "-> $(URL_STREAMLIT)"

open-prom:
	@$(OPEN_CMD) "$(URL_PROM)" >/dev/null 2>&1 & echo "-> $(URL_PROM)"

open-kafkaui:
	@$(OPEN_CMD) "$(URL_KAFKAUI)" >/dev/null 2>&1 & echo "-> $(URL_KAFKAUI)"

open-hdfs-nn:
	@$(OPEN_CMD) "$(URL_HDFS_NN)" >/dev/null 2>&1 & echo "-> $(URL_HDFS_NN)"

open-hdfs-dn:
	@$(OPEN_CMD) "$(URL_HDFS_DN)" >/dev/null 2>&1 & echo "-> $(URL_HDFS_DN)"

check-env:
	@echo "-> ENV:"
	@echo "  TZ=$(TZ)"
	@echo "  HOST=$(HOST)"
	@echo "  KAFKA_BROKER=$(KAFKA_BROKER)"
	@echo "  TOPIC_RAW=$(TOPIC_RAW)"
	@echo "  TOPIC_HOURLY=$(TOPIC_HOURLY)"
	@echo "  TOPIC_CURRENT=$(TOPIC_CURRENT)"
	@echo "  TOPIC_Q_REQ=$(TOPIC_Q_REQ)"
	@echo "  TOPIC_Q_RES=$(TOPIC_Q_RES)"
	@echo "  URLs:"
	@echo "   $(URL_GRAFANA)"
	@echo "   $(URL_AIRFLOW)"
	@echo "   $(URL_SPARK)"
	@echo "   $(URL_HDFS_NN)"
	@echo "   $(URL_HDFS_DN)"
	@echo "   $(URL_STREAMLIT)"
	@echo "   $(URL_PROM)"
	@echo "   $(URL_KAFKAUI)"

clean-all:
	@echo "-> down -v --remove-orphans"
	@docker compose down -v --remove-orphans

nuke:
	@echo "-> system prune -af --volumes"
	@docker system prune -af --volumes
