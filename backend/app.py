from fastapi import FastAPI, Header
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import json, os

app = FastAPI(title="OpenMeteo Backend")
CITIES_FILE = "/app/conf/cities.json"

# ---- Prometheus
registry = CollectorRegistry()
CITY_EVENTS = Counter(
    "openmeteo_city_events_total",
    "Dummy events per city (visible in Grafana)",
    ["city_id", "city_name", "country"],
    registry=registry,
)

# ---- Models
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

def _load_config() -> ConfigDoc:
    if not os.path.exists(CITIES_FILE):
        return ConfigDoc(version=1, updated_at=_now(), updated_by="backend", cities=[])
    with open(CITIES_FILE, "r") as f:
        data = json.load(f)
    # backward compat: allow dict with 'cities'
    return ConfigDoc(**data)

def _save_config(doc: ConfigDoc):
    os.makedirs(os.path.dirname(CITIES_FILE), exist_ok=True)
    with open(CITIES_FILE, "w") as f:
        json.dump(json.loads(doc.model_dump_json()), f, indent=2)

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _upsert_city(doc: ConfigDoc, city: City) -> ConfigDoc:
    exists = {c.id for c in doc.cities}
    if city.id in exists:
        doc.cities = [city if c.id == city.id else c for c in doc.cities]
    else:
        doc.cities.append(city)
    doc.version = (doc.version or 0) + 1
    doc.updated_at = _now()
    return doc

# ---- Endpoints
@app.get("/")
def hello():
    return {"hello": "backend", "hint": "GET/POST /config/cities, GET /metrics"}

@app.get("/config/cities")
def list_cities() -> Dict[str, Any]:
    doc = _load_config()
    return json.loads(doc.model_dump_json())

@app.post("/config/cities")
def add_or_update_city(city: City, x_user: Optional[str] = Header(default="streamlit")) -> Dict[str, Any]:
    doc = _load_config()
    doc = _upsert_city(doc, city)
    doc.updated_by = x_user or "streamlit"
    _save_config(doc)
    # ensure labels appear ASAP
    if city.active:
        CITY_EVENTS.labels(city.id, city.name, city.country).inc()
    else:
        CITY_EVENTS.labels(city.id, city.name, city.country).inc(0)
    return {"ok": True, "config": json.loads(doc.model_dump_json())}

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    # expose at least one sample per active city so Grafana sees labels
    doc = _load_config()
    for c in doc.cities:
        if c.active:
            CITY_EVENTS.labels(c.id, c.name, c.country).inc(0)
    return PlainTextResponse(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
