import os
import json
import time
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from kafka import KafkaProducer
from prometheus_client import (
    Counter, Gauge, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

# -------------------------
# Configuration (ENV)
# -------------------------
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "weather.raw.openmeteo")
CITIES_FILE = os.getenv("CITIES_FILE", "/app/conf/cities.json")
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
OPENMETEO_BASE_URL = os.getenv(
    "OPENMETEO_BASE_URL", "https://api.open-meteo.com/v1/forecast"
)
# Paramètres Open-Meteo (simples par défaut, ajustables via ENV si besoin)
CURRENT_PARAMS = os.getenv(
    "OPENMETEO_CURRENT_PARAMS",
    "temperature_2m,relative_humidity_2m,wind_speed_10m"
)
HOURLY_PARAMS = os.getenv(
    "OPENMETEO_HOURLY_PARAMS",
    "temperature_2m,relative_humidity_2m,wind_speed_10m"
)
OPENMETEO_TZ = os.getenv("OPENMETEO_TZ", "UTC")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("openmeteo_producer")

# -------------------------
# Prometheus metrics
# -------------------------
registry = CollectorRegistry()
CITY_EVENTS = Counter(
    "openmeteo_city_events_total",
    "Dummy events produced per city (also used as a presence signal for Grafana)",
    ["city_id", "city_name", "country"],
    registry=registry,
)
ITER_SENT = Gauge(
    "openmeteo_iteration_sent",
    "Number of envelopes sent in the last iteration",
    registry=registry,
)
LAST_ITER_TS = Gauge(
    "openmeteo_last_iteration_timestamp",
    "Unix timestamp of last iteration end",
    registry=registry,
)

# -------------------------
# Helpers
# -------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load_cities_config(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cities = data.get("cities", [])
        return [c for c in cities if c.get("active", True)]
    except FileNotFoundError:
        log.warning("Cities config not found: %s", path)
        return []
    except Exception as e:
        log.error("Failed to read cities config %s: %s", path, e)
        return []

def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
        retries=3,
        max_in_flight_requests_per_connection=1,
    )

def fetch_city_weather(client: httpx.Client, city: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current": CURRENT_PARAMS,
        "hourly": HOURLY_PARAMS,
        "timezone": OPENMETEO_TZ,
    }
    try:
        resp = client.get(OPENMETEO_BASE_URL, params=params, timeout=20)
        if resp.status_code >= 400:
            log.warning("Open-Meteo error %s for city %s: %s",
                        resp.status_code, city.get("id"), resp.text[:300])
            return None
        payload = resp.json()
        envelope = {
            "_meta": {
                "source": "open-meteo",
                "ingested_at": _now_iso(),
                "city_id": city.get("id"),
                "city_name": city.get("name"),
                "country": city.get("country"),
            },
            "raw": payload,
        }
        return envelope
    except Exception as exc:
        log.warning("Failed to fetch for city %s: %s", city.get("id"), exc)
        return None

# -------------------------
# Producer loop (thread)
# -------------------------
def loop_produce():
    log.info("Starting Open-Meteo producer")
    log.info("broker=%s topic=%s cities_file=%s", BROKER, TOPIC_RAW, CITIES_FILE)

    producer: Optional[KafkaProducer] = None

    # un seul client HTTP réutilisé
    with httpx.Client() as client:
        iteration = 0
        while True:
            start = time.time()
            sent = 0

            # recharge la config à chaque boucle
            cities = load_cities_config(CITIES_FILE)
            if not cities:
                log.info("No active cities configured (file=%s)", CITIES_FILE)

            # (ré)initialise le producer au besoin
            if producer is None:
                try:
                    producer = create_kafka_producer()
                except Exception as e:
                    log.error("KafkaProducer init failed: %s", e)
                    producer = None  # retentera au tour suivant

            for c in cities:
                # Expose immédiatement les labels Prometheus pour Grafana (même sans envoi)
                CITY_EVENTS.labels(c["id"], c["name"], c["country"]).inc(0)

                env = fetch_city_weather(client, c)
                if env is None:
                    continue

                # Si pas de producer dispo, saute l'envoi
                if producer is None:
                    continue

                try:
                    producer.send(TOPIC_RAW, value=env)
                    # inc() visible dans Grafana pour marquer de l'activité
                    CITY_EVENTS.labels(c["id"], c["name"], c["country"]).inc()
                    sent += 1
                except Exception as exc:
                    log.error("Kafka send failed for city %s: %s", c.get("id"), exc)
                    # Force une ré-init au prochain tour
                    try:
                        producer.close(timeout=2)
                    except Exception:
                        pass
                    producer = None

            # flush best-effort
            if producer is not None:
                try:
                    producer.flush(timeout=10)
                except Exception:
                    pass

            duration = time.time() - start
            ITER_SENT.set(sent)
            LAST_ITER_TS.set(time.time())
            log.info("iteration=%d sent=%d duration_sec=%.2f", iteration, sent, duration)
            iteration += 1

            time.sleep(max(1, FETCH_INTERVAL_SECONDS))

# -------------------------
# HTTP server (health + metrics)
# -------------------------
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            data = generate_latest(registry)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(data)
        elif self.path in ("/", "/health", "/ready"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"hello producer")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")

def main():
    threading.Thread(target=loop_produce, daemon=True).start()
    port = int(os.getenv("PRODUCER_HTTP_PORT", "8000"))
    log.info("Starting producer HTTP server on :%d", port)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

if __name__ == "__main__":
    main()
