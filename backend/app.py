# backend/app.py
from fastapi import FastAPI, HTTPException, Header, Response, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import List, Optional
from prometheus_client import Counter, Gauge, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from datetime import datetime, timezone
import os, json, uuid, threading, time

from kafka import KafkaProducer, KafkaConsumer
from models import City, ConfigDoc, slugify

# --------------------
# App & configuration
# --------------------
app = FastAPI(title="OpenMeteo Backend")

CITIES_FILE = os.getenv("CITIES_FILE", "/app/conf/cities.json")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_Q_REQ = os.getenv("TOPIC_Q_REQ", "weather.queries.requests")
TOPIC_Q_RES = os.getenv("TOPIC_Q_RES", "weather.queries.results")
TOPIC_HOURLY = os.getenv("TOPIC_HOURLY", "weather.hourly.flattened")

# -----------
# Prometheus
# -----------
REG = CollectorRegistry()
REQS = Counter("backend_requests_total", "Total HTTP requests", ["path", "method"], registry=REG)
CFG_VERSION = Gauge("backend_config_version", "Current config version", registry=REG)
CFG_LAST_UPDATE = Gauge("backend_config_last_update_ts", "Last config updated_at (unix ts)", registry=REG)

# métriques temps réel (valeur la plus récente par ville)
REALTIME_TEMP = Gauge("weather_temperature_2m", "Temp °C (latest)", ["city_id","city_name","country"], registry=REG)
REALTIME_HUM  = Gauge("weather_relative_humidity_2m", "RH % (latest)", ["city_id","city_name","country"], registry=REG)
REALTIME_WIND = Gauge("weather_wind_speed_10m", "Wind m/s (latest)", ["city_id","city_name","country"], registry=REG)

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _load_config() -> ConfigDoc:
    if not os.path.exists(CITIES_FILE):
        doc = ConfigDoc(version=1, updated_at=_now(), updated_by="bootstrap", cities=[])
        _save_config(doc)
        return doc
    with open(CITIES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return ConfigDoc(**data)

def _save_config(doc: ConfigDoc):
    os.makedirs(os.path.dirname(CITIES_FILE), exist_ok=True)
    with open(CITIES_FILE, "w", encoding="utf-8") as f:
        json.dump(json.loads(doc.model_dump_json()), f, indent=2)
    CFG_VERSION.set(doc.version)
    try:
        CFG_LAST_UPDATE.set(datetime.fromisoformat(doc.updated_at.replace("Z","")).timestamp())
    except Exception:
        pass

def _etag_for(doc: ConfigDoc) -> str:
    return f'W/"v{doc.version}-n{len(doc.cities)}"'

def _upsert_city(doc: ConfigDoc, city: City) -> ConfigDoc:
    seen = False
    out = []
    for c in doc.cities:
        if c.id == city.id:
            out.append(city)
            seen = True
        else:
            out.append(c)
    if not seen:
        out.append(city)
    doc.cities = out
    return doc

# -------------
# Config APIs
# -------------
@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

@app.get("/metrics")
def metrics():
    return Response(generate_latest(REG), media_type=CONTENT_TYPE_LATEST)

@app.get("/config/cities")
def list_cities(request: Request):
    REQS.labels("/config/cities", "GET").inc()
    doc = _load_config()
    etag = _etag_for(doc)
    inm = request.headers.get("if-none-match")
    if inm and inm == etag:
        return Response(status_code=304)
    return Response(
        content=json.dumps(json.loads(doc.model_dump_json()), indent=2),
        media_type="application/json",
        headers={"ETag": etag}
    )

@app.post("/config/cities")
def create_city(city: City, x_user: str = Header(default="streamlit")):
    REQS.labels("/config/cities", "POST").inc()
    doc = _load_config()
    city.id = slugify(f"{city.name}-{city.country}")
    doc = _upsert_city(doc, city)
    doc.version += 1
    doc.updated_at = _now()
    doc.updated_by = x_user
    _save_config(doc)
    return {"ok": True, "city": json.loads(city.model_dump_json()), "version": doc.version}

@app.put("/config/cities/{city_id}")
def update_city(city_id: str, city: City, x_user: str = Header(default="streamlit")):
    REQS.labels("/config/cities/{id}", "PUT").inc()
    doc = _load_config()
    if city.id != city_id:
        city.id = city_id
    ids = {c.id for c in doc.cities}
    if city_id not in ids:
        raise HTTPException(status_code=404, detail="city not found")
    doc = _upsert_city(doc, city)
    doc.version += 1
    doc.updated_at = _now()
    doc.updated_by = x_user
    _save_config(doc)
    return {"ok": True, "city": json.loads(city.model_dump_json()), "version": doc.version}

@app.delete("/config/cities/{city_id}")
def delete_city(city_id: str, x_user: str = Header(default="streamlit")):
    REQS.labels("/config/cities/{id}", "DELETE").inc()
    doc = _load_config()
    before = len(doc.cities)
    doc.cities = [c for c in doc.cities if c.id != city_id]
    after = len(doc.cities)
    if before == after:
        raise HTTPException(status_code=404, detail="city not found")
    doc.version += 1
    doc.updated_at = _now()
    doc.updated_by = x_user
    _save_config(doc)
    return {"ok": True, "deleted": city_id, "version": doc.version}

# -----------------------
# Batch requests via Kafka (inchangé)
# -----------------------
class QueryRequest(BaseModel):
    request_id: Optional[str] = None
    city_ids: List[str] = []
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    metrics: List[str] = ["temperature_2m"]
    agg: str = "avg"

_producer: Optional[KafkaProducer] = None
def _kafka_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=20, acks=1,
        )
    return _producer

@app.post("/query")
def post_query(req: QueryRequest, x_user: str = Header(default="streamlit")):
    REQS.labels("/query", "POST").inc()
    rid = req.request_id or str(uuid.uuid4())
    payload = {
        "request_id": rid,
        "city_ids": req.city_ids,
        "date_from": req.date_from,
        "date_to": req.date_to,
        "metrics": req.metrics,
        "agg": req.agg,
        "submitted_by": x_user,
        "submitted_at": _now(),
    }
    _kafka_producer().send(TOPIC_Q_REQ, value=payload)
    _kafka_producer().flush()
    return {"request_id": rid, "status": "submitted"}

@app.get("/query/{request_id}")
def get_query_status(request_id: str):
    REQS.labels("/query/{id}", "GET").inc()
    try:
        consumer = KafkaConsumer(
            TOPIC_Q_RES,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1200,
        )
        latest = None
        for msg in consumer:
            val = msg.value
            if isinstance(val, dict) and val.get("request_id") == request_id:
                latest = val
        consumer.close()
        if latest is None:
            return {"request_id": request_id, "status": "pending"}
        return latest
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Kafka read failed: {exc}")

# -----------------------
# Temps réel -> Prometheus (TOPIC_HOURLY consumer)
# -----------------------
def _realtime_consumer_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_HOURLY,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=30000,
            )
            for msg in consumer:
                v = msg.value or {}
                city = (v.get("city_id"), v.get("city_name"), v.get("country"))
                t  = v.get("temperature_2m")
                h  = v.get("relative_humidity_2m")
                w  = v.get("wind_speed_10m")
                if city[0]:
                    if t is not None: REALTIME_TEMP.labels(*city).set(float(t))
                    if h is not None: REALTIME_HUM.labels(*city).set(float(h))
                    if w is not None: REALTIME_WIND.labels(*city).set(float(w))
        except Exception:
            time.sleep(3)  # backoff et on repart

# démarre le thread consumer au boot
threading.Thread(target=_realtime_consumer_loop, daemon=True).start()
