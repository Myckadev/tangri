# backend/app.py
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import os, json, uuid

# Kafka
from kafka import KafkaProducer, KafkaConsumer

# --------------------
# App & configuration
# --------------------
app = FastAPI(title="OpenMeteo Backend")

# Chemins / ENV
CONF_DIR     = os.getenv("CONF_DIR", "/app/conf")
CITIES_FILE  = os.path.join(CONF_DIR, "cities.json")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_Q_REQ  = os.getenv("TOPIC_Q_REQ", "weather.queries.requests")
TOPIC_Q_RES  = os.getenv("TOPIC_Q_RES", "weather.queries.results")

# -----------
# Prometheus
# -----------
registry = CollectorRegistry()
CITY_EVENTS = Counter(
    "openmeteo_city_events_total",
    "Dummy events per city (visible in Grafana)",
    ["city_id", "city_name", "country"],
    registry=registry,
)

# ----------------
# Pydantic models
# ----------------
class City(BaseModel):
    id: str
    name: str
    country: str = Field(min_length=2, max_length=3)
    lat: float
    lon: float
    active: bool = True

class ConfigDoc(BaseModel):
    version: int
    updated_at: str
    updated_by: str
    cities: List[City] = []

class Query(BaseModel):
    city_ids: List[str] = []
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    metrics: List[str] = []
    agg: Optional[str] = None

# --------------
# Kafka helpers
# --------------
_producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            linger_ms=20,
            acks=1,
        )
    return _producer

# ---------------
# Config helpers
# ---------------
def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _load_config() -> ConfigDoc:
    if not os.path.exists(CITIES_FILE):
        # doc par défaut si absent
        return ConfigDoc(version=1, updated_at=_now(), updated_by="backend", cities=[])
    with open(CITIES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    # tolère un dict “brut”
    return ConfigDoc(**data)

def _save_config(doc: ConfigDoc):
    os.makedirs(os.path.dirname(CITIES_FILE), exist_ok=True)
    with open(CITIES_FILE, "w", encoding="utf-8") as f:
        json.dump(json.loads(doc.model_dump_json()), f, indent=2)

def _upsert_city(doc: ConfigDoc, city: City) -> ConfigDoc:
    ids = {c.id for c in doc.cities}
    if city.id in ids:
        doc.cities = [city if c.id == city.id else c for c in doc.cities]
    else:
        doc.cities.append(city)
    doc.version = (doc.version or 0) + 1
    doc.updated_at = _now()
    return doc

# ---------
# Endpoints
# ---------
@app.get("/")
def root():
    return {
        "message": "hello from backend",
        "ts": _now(),
        "hint": "GET /config/cities, POST /config/cities, GET /metrics, POST /query, GET /query/{request_id}"
    }

@app.get("/health")
def health():
    return {"status": "ok", "message": "hello", "ts": _now()}

# ---- Config villes
@app.get("/config/cities")
def get_cities() -> Dict[str, Any]:
    doc = _load_config()
    return json.loads(doc.model_dump_json())

@app.post("/config/cities")
def add_or_update_city(city: City, x_user: Optional[str] = Header(default="streamlit")) -> Dict[str, Any]:
    doc = _load_config()
    doc = _upsert_city(doc, city)
    doc.updated_by = x_user or "streamlit"
    _save_config(doc)
    # pousse/maintient les labels prometheus visibles
    if city.active:
        CITY_EVENTS.labels(city.id, city.name, city.country).inc()
    else:
        CITY_EVENTS.labels(city.id, city.name, city.country).inc(0)
    return {"ok": True, "config": json.loads(doc.model_dump_json())}

# ---- Prometheus
@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> PlainTextResponse:
    # garantit au moins un sample par ville active
    doc = _load_config()
    for c in doc.cities:
        if c.active:
            CITY_EVENTS.labels(c.id, c.name, c.country).inc(0)
    return PlainTextResponse(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

# ---- Kafka queries
@app.post("/query")
def submit_query(q: Query):
    request_id = str(uuid.uuid4())
    payload = {
        "request_id": request_id,
        "submitted_at": _now(),
        "query": q.model_dump(),
    }
    try:
        get_producer().send(TOPIC_Q_REQ, key=request_id, value=payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Kafka send failed: {exc}")
    return {"message": "query accepted", "request_id": request_id}

@app.get("/query/{request_id}")
def query_status(request_id: str):
    """
    Stratégie simple: on lit le topic réponses et on renvoie
    la dernière occurrence correspondant au request_id.
    Attention: c'est un GET "blocking-ish" avec consumer_timeout_ms court.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_Q_RES,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=2500,  # court pour éviter de bloquer trop longtemps
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
