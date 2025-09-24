import json, os, time, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Any
from prometheus_client import Counter, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from kafka import KafkaProducer

CITIES_FILE = "/app/conf/cities.json"
BROKER = os.getenv("KAFKA_BROKER","kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW","weather.raw.openmeteo")

registry = CollectorRegistry()
CITY_EVENTS = Counter(
    "openmeteo_city_events_total",
    "Dummy events produced per city",
    ["city_id", "city_name", "country"],
    registry=registry,
)

def load_config() -> Dict[str, Any]:
    try:
        with open(CITIES_FILE,"r") as f:
            return json.load(f)
    except Exception:
        return {"version":1,"updated_at":"","updated_by":"producer","cities":[]}

def loop_produce():
    prod = None
    while True:
        try:
            if prod is None:
                prod = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode())
            doc = load_config()
            for c in doc.get("cities", []):
                if c.get("active", True):
                    CITY_EVENTS.labels(c["id"], c["name"], c["country"]).inc()
                    prod.send(TOPIC_RAW, {
                        "hello":"openmeteo",
                        "city_id": c["id"],
                        "city_name": c["name"],
                        "country": c["country"],
                        "lat": c["lat"],
                        "lon": c["lon"],
                    })
            prod.flush()
        except Exception:
            prod = None
        time.sleep(10)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            data = generate_latest(registry)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"hello producer")

if __name__ == "__main__":
    threading.Thread(target=loop_produce, daemon=True).start()
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
