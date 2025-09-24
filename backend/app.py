from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="OpenMeteo Backend Hello")

class Query(BaseModel):
    city_ids: list[str] = []
    date_from: str | None = None
    date_to: str | None = None
    metrics: list[str] = []
    agg: str | None = None

@app.get("/")
def root():
    return {"message": "hello from backend", "ts": datetime.utcnow().isoformat()}

@app.get("/health")
def health():
    return {"status": "ok", "message": "hello", "ts": datetime.utcnow().isoformat()}

@app.post("/query")
def submit_query(q: Query):
    return {"message": "hello query received", "payload": q.model_dump(), "ts": datetime.utcnow().isoformat()}

@app.get("/config/cities")
def get_cities():
    # Stub (pas de lecture disque ici)
    return {
        "version": 1,
        "updated_at": datetime.utcnow().isoformat(),
        "updated_by": "hello",
        "cities": [{"id":"paris-fr","name":"Paris","country":"FR","lat":48.8566,"lon":2.3522,"active":True}]
    }
