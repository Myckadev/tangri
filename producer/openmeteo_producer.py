# producer/openmeteo_producer.py
import os
import re
import json
import time
import signal
import queue
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from kafka import KafkaProducer
from prometheus_client import (
    Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# --------------------------------------------------
# Env
# --------------------------------------------------
BROKER                  = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW               = os.getenv("TOPIC_RAW", "weather.raw.openmeteo")
TOPIC_CURRENT           = os.getenv("TOPIC_CURRENT", "weather.current.metrics")
BACKEND_URL             = os.getenv("BACKEND_URL", "http://backend:8000")
FETCH_INTERVAL_SECONDS  = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))
HTTP_PORT               = int(os.getenv("PRODUCER_HTTP_PORT", "8000"))

OPENMETEO_BASE          = os.getenv("OPENMETEO_BASE", "https://api.open-meteo.com/v1/forecast")
OPENMETEO_TIMEOUT       = float(os.getenv("OPENMETEO_TIMEOUT", "10"))

# --------------------------------------------------
# Prometheus
# --------------------------------------------------
REG = CollectorRegistry()

REQ_TOTAL = Counter("openmeteo_requests_total",
                    "Total Open-Meteo HTTP calls", ["kind"], registry=REG)
REQ_FAIL  = Counter("openmeteo_requests_fail_total",
                    "Failed Open-Meteo HTTP calls", ["kind", "reason"], registry=REG)
REQ_LAT   = Histogram("openmeteo_request_seconds",
                      "Latency of Open-Meteo API calls", ["kind"], registry=REG)

PROD_SENT   = Counter("producer_kafka_messages_total",
                      "Kafka messages produced", ["topic"], registry=REG)
PROD_FAIL   = Counter("producer_kafka_fail_total",
                      "Kafka produce failures", ["topic", "reason"], registry=REG)
ITER_SENT   = Gauge("producer_iteration_sent", "Events sent this iteration", registry=REG)
LAST_ITER_TS= Gauge("producer_last_iteration_ts", "Last iteration unix ts", registry=REG)

CFG_VERSION = Gauge("producer_config_version", "Config version observed", registry=REG)
CFG_REFRESH = Gauge("producer_last_config_refresh_ts", "Last config refresh ts", registry=REG)
CFG_CITIES  = Gauge("producer_config_cities", "Number of active cities", registry=REG)
PAUSED      = Gauge("producer_paused", "1 if paused else 0", registry=REG)

# Météo live (pour Grafana “instantané”)
TEMP = Gauge("openmeteo_temperature_celsius",
             "Latest temperature (current) by city", ["city_id", "city_name"], registry=REG)
HUM  = Gauge("openmeteo_humidity_percent",
             "Latest humidity (current) by city", ["city_id", "city_name"], registry=REG)
WIND = Gauge("openmeteo_windspeed_ms",
             "Latest wind speed (current, m/s) by city", ["city_id", "city_name"], registry=REG)

CITY_EVENTS = Counter("openmeteo_city_events_total",
                      "Total produced events per city (signal for Grafana)",
                      ["city_id", "city_name", "country"], registry=REG)

# --------------------------------------------------
# State
# --------------------------------------------------
_running = True
_paused  = False

_cities: List[Dict[str, Any]] = []
_etag: Optional[str] = None
_config_version = 0

# Producer management with auto-recreate
_producer_lock = threading.Lock()
_producer: Optional[KafkaProducer] = None

# Single client shared (thread-safe)
_http_client = httpx.Client(timeout=OPENMETEO_TIMEOUT, http2=True)

# Work queue for one-off fetches (e.g., /now?city_id=...)
_now_queue: "queue.Queue[str]" = queue.Queue(maxsize=256)

# --------------------------------------------------
# Utils
# --------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "na"

def _get_producer() -> KafkaProducer:
    global _producer
    with _producer_lock:
        if _producer is None:
            log.info("KafkaProducer init…")
            _producer = KafkaProducer(
                bootstrap_servers=[BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                acks=1,
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
        return _producer

def _recreate_producer(e: Exception):
    global _producer
    with _producer_lock:
        try:
            if _producer:
                _producer.close(timeout=2)
        except Exception:
            pass
        _producer = None
    log.warning("KafkaProducer recreated after error: %s", e)

# --------------------------------------------------
# Open-Meteo calls
# --------------------------------------------------
def fetch_openmeteo(city: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Récupère à la fois:
      - current: temperature_2m, relative_humidity_2m, wind_speed_10m
      - hourly:  temperature_2m, relative_humidity_2m, wind_speed_10m (24h)
    """
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "hourly":  "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "forecast_days": 1,
        "timezone": "UTC",
    }
    kind = "current+hourly"
    t0 = time.time()
    try:
        r = _http_client.get(OPENMETEO_BASE, params=params)
        REQ_TOTAL.labels(kind=kind).inc()
        r.raise_for_status()
        REQ_LAT.labels(kind=kind).observe(time.time() - t0)
        data = r.json()
        return {
            "_meta": {
                "city_id": city["id"],
                "city_name": city["name"],
                "country": city["country"],
                "ingested_at": _now_iso(),
            },
            "source": "open-meteo",
            "current": data.get("current"),
            "hourly": data.get("hourly"),
            "raw": data,
        }
    except Exception as e:
        REQ_FAIL.labels(kind=kind, reason=type(e).__name__).inc()
        log.warning("OpenMeteo fetch failed for %s: %s", city.get("id"), e)
        return None

def extract_current_flat(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Tente d'extraire les valeurs “current”.
    Si absent, fallback sur la dernière valeur “hourly”.
    """
    meta = event.get("_meta", {})
    cur  = event.get("current") or {}

    def value_from_hourly(key: str) -> Optional[float]:
        h = event.get("hourly") or {}
        vals = h.get(key) or []
        if not vals:
            return None
        try:
            return float(vals[-1])
        except Exception:
            return None

    temp = cur.get("temperature_2m")
    hum  = cur.get("relative_humidity_2m")
    wind = cur.get("wind_speed_10m")

    # fallback if needed
    temp = temp if temp is not None else value_from_hourly("temperature_2m")
    hum  = hum  if hum  is not None else value_from_hourly("relative_humidity_2m")
    wind = wind if wind is not None else value_from_hourly("wind_speed_10m")

    if temp is None and hum is None and wind is None:
        return None

    return {
        "city_id": meta.get("city_id"),
        "city_name": meta.get("city_name"),
        "country": meta.get("country"),
        "ts": _now_iso(),
        "temperature_c": temp,
        "humidity_pct": hum,
        "wind_ms": wind,
        "source": "open-meteo",
    }

# --------------------------------------------------
# Kafka produce
# --------------------------------------------------
def send_kafka(topic: str, value: Dict[str, Any]):
    try:
        p = _get_producer()
        p.send(topic, value=value)
        PROD_SENT.labels(topic=topic).inc()
    except Exception as e:
        PROD_FAIL.labels(topic=topic, reason=type(e).__name__).inc()
        log.error("Kafka send failed [%s]: %s", topic, e)
        _recreate_producer(e)

# --------------------------------------------------
# Config polling
# --------------------------------------------------
def poll_config_loop():
    global _cities, _etag, _config_version
    client = httpx.Client(timeout=5)
    while _running:
        try:
            headers = {"If-None-Match": _etag} if _etag else {}
            r = client.get(f"{BACKEND_URL}/config/cities", headers=headers)
            if r.status_code == 200:
                doc = r.json()
                _etag = r.headers.get("ETag")
                _config_version = int(doc.get("version", 0))
                cities = [c for c in doc.get("cities", []) if c.get("active", True)]
                # Normalisation
                for c in cities:
                    c["id"] = _slug(c["id"])
                    c["name"] = c.get("name") or c["id"]
                    c["country"] = c.get("country") or "na"
                _cities = cities
                CFG_VERSION.set(_config_version)
                CFG_CITIES.set(len(_cities))
                CFG_REFRESH.set(time.time())
                log.info("Config refreshed: %d cities (v%d)", len(_cities), _config_version)
            elif r.status_code == 304:
                CFG_REFRESH.set(time.time())
        except Exception as e:
            log.warning("Config poll failed: %s", e)
        time.sleep(30)

# --------------------------------------------------
# Main loop (streaming)
# --------------------------------------------------
def streaming_loop():
    global _paused
    iteration = 0
    PAUSED.set(0)
    while _running:
        if _paused:
            PAUSED.set(1)
            time.sleep(0.5)
            continue
        PAUSED.set(0)

        sent = 0
        t_iter = time.time()

        for city in list(_cities):
            event = fetch_openmeteo(city)
            if not event:
                continue

            # 1) brut
            send_kafka(TOPIC_RAW, event)

            # 2) aplati “current” -> TOPIC_CURRENT + prom gauges
            flat = extract_current_flat(event)
            if flat:
                send_kafka(TOPIC_CURRENT, flat)

                cid  = flat["city_id"]
                cname= flat["city_name"]
                if flat.get("temperature_c") is not None:
                    TEMP.labels(city_id=cid, city_name=cname).set(float(flat["temperature_c"]))
                if flat.get("humidity_pct") is not None:
                    HUM.labels(city_id=cid, city_name=cname).set(float(flat["humidity_pct"]))
                if flat.get("wind_ms") is not None:
                    WIND.labels(city_id=cid, city_name=cname).set(float(flat["wind_ms"]))

                CITY_EVENTS.labels(cid, cname, flat.get("country") or "na").inc()

                sent += 2  # RAW + CURRENT
            else:
                sent += 1  # RAW only

        ITER_SENT.set(sent)
        LAST_ITER_TS.set(time.time())

        log.info("iteration=%d sent=%d duration_sec=%.2f", iteration, sent, time.time() - t_iter)
        iteration += 1

        # Drain des requêtes ponctuelles /now
        _drain_now_queue()

        time.sleep(max(1, FETCH_INTERVAL_SECONDS))

def _drain_now_queue():
    """Traite les /now?city_id=… demandés via HTTP; non bloquant."""
    try:
        while True:
            cid = _now_queue.get_nowait()
            city = next((c for c in _cities if c["id"] == cid), None)
            if not city:
                log.info("now: city %s not found in config", cid)
                continue
            ev = fetch_openmeteo(city)
            if not ev:
                continue
            send_kafka(TOPIC_RAW, ev)
            flat = extract_current_flat(ev)
            if flat:
                send_kafka(TOPIC_CURRENT, flat)
                TEMP.labels(city_id=flat["city_id"], city_name=flat["city_name"]).set(float(flat.get("temperature_c") or 0.0))
                if flat.get("humidity_pct") is not None:
                    HUM.labels(city_id=flat["city_id"], city_name=flat["city_name"]).set(float(flat["humidity_pct"]))
                if flat.get("wind_ms") is not None:
                    WIND.labels(city_id=flat["city_id"], city_name=flat["city_name"]).set(float(flat["wind_ms"]))
                CITY_EVENTS.labels(flat["city_id"], flat["city_name"], flat.get("country") or "na").inc()
    except queue.Empty:
        return

# --------------------------------------------------
# HTTP server
# --------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def _write_json(self, payload: Any, code: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        global _paused
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            qs   = parse_qs(parsed.query)

            if path == "/metrics":
                body = generate_latest(REG)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/health":
                self._write_json({"status": "ok", "ts": _now_iso()})
                return

            if path == "/pause":
                _paused = True
                PAUSED.set(1)
                self._write_json({"paused": True})
                return

            if path == "/resume":
                _paused = False
                PAUSED.set(0)
                self._write_json({"paused": False})
                return

            if path == "/cities":
                self._write_json({"version": _config_version, "cities": _cities})
                return

            if path == "/now":
                cid = (qs.get("city_id") or [""])[0].strip().lower()
                if not cid:
                    self._write_json({"error": "missing city_id"}, 400)
                    return
                try:
                    _now_queue.put_nowait(cid)
                    self._write_json({"queued": True, "city_id": cid})
                except queue.Full:
                    self._write_json({"queued": False, "error": "queue full"}, 503)
                return

            self._write_json({"error": "not found"}, 404)

        except Exception as e:
            log.exception("HTTP error")
            self._write_json({"error": str(e)}, 500)

# --------------------------------------------------
# Main
# --------------------------------------------------
def shutdown(signum, frame):
    global _running
    log.info("Signal %s received, shutting down…", signum)
    _running = False

def main():
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    threading.Thread(target=poll_config_loop, daemon=True).start()
    threading.Thread(target=streaming_loop, daemon=True).start()

    log.info("Starting producer HTTP server on :%d", HTTP_PORT)
    HTTPServer(("0.0.0.0", HTTP_PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
