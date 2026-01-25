import os
import time
import json
import requests
# Explicitly importing everything needed to avoid NameErrors
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
API_KEY = os.getenv("API_KEY", "")
MOCK_MODE = os.getenv("MOCK_MODE", "true").lower() == "true"
TOPIC = "weather_raw"

LOCATIONS = [
    {"lat": 52.084516, "lon": 5.115539, "name": "Sensorfact Utrecht"},
    {"lat": 52.3676, "lon": 4.9041, "name": "Amsterdam"},
    {"lat": 51.9225, "lon": 4.47917, "name": "Rotterdam"}
]

def ensure_topic_exists():
    """Waits for Kafka to be ready and creates the topic."""
    print(f"Attempting to connect to Kafka at {KAFKA_BROKER}...")
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                request_timeout_ms=5000,
                connection_timeout_ms=5000
            )
            topic_list = [NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)]
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                print(f"Topic '{TOPIC}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{TOPIC}' already exists.")
            finally:
                admin_client.close()
            break
        except Exception as e:
            print(f"Kafka not ready yet... ({e}). Retrying in 5s...")
            time.sleep(5)

def fetch_weather(lat, lon):
    if MOCK_MODE:
        # Generate some fake precipitation data for testing
        return {
            "lat": lat, "lon": lon,
            "minutely": [{"dt": int(time.time()) + (i*60), "precipitation": 0.1} for i in range(60)]
        }
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,daily,current&units=metric&appid={API_KEY}"
    return requests.get(url).json()

def main():
    # 1. Wait for Kafka and create topic
    ensure_topic_exists()

    # 2. Initialize Producer
    print("Initializing Kafka Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Ensure data is written to lead
        retries=5
    )

    print("Ingestion loop started.")
    while True:
        for loc in LOCATIONS:
            try:
                data = fetch_weather(loc['lat'], loc['lon'])
                data['location_name'] = loc['name']
                # Use location name as key for partitioning
                producer.send(TOPIC, key=loc['name'].encode('utf-8'), value=data)
                print(f"Dispatched forecast for {loc['name']}")
            except Exception as e:
                print(f"Failed to fetch/send data for {loc['name']}: {e}")

        producer.flush()
        time.sleep(60)

if __name__ == "__main__":
    main()