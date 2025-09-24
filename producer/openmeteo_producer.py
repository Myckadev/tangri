import time
import os

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC_RAW", "weather.raw.openmeteo")
CITIES_FILE = os.getenv("CITIES_FILE", "/app/conf/cities.json")

def main():
    print("hello from producer")
    print(f"broker={BROKER} topic={TOPIC} cities_file={CITIES_FILE}", flush=True)
    # boucle légère pour rester vivant en démo
    i = 0
    while True:
        print(f"[producer] heartbeat hello {i}", flush=True)
        i += 1
        time.sleep(10)

if __name__ == "__main__":
    main()
