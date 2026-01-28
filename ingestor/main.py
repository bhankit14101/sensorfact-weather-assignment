import os
import asyncio
import json
import logging
import csv
import time
import random
from aiohttp import ClientSession, ClientTimeout
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestor")

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
API_KEY = os.getenv("OPENWEATHER_API_KEY")
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"
TOPIC_NAME = "weather_raw"
LOCATIONS_FILE = "locations.csv"

def load_locations():
    try:
        with open(LOCATIONS_FILE, mode='r', encoding='utf-8') as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logger.error(f"Failed to load locations: {e}")
        return [{"name": "Default", "lat": 52.0845, "lon": 5.1155}]

def ensure_topic_exists():
    """Ensures the Kafka topic exists before ingestion starts."""
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='weather-admin'
            )
            topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic '{TOPIC_NAME}' created.")
            admin_client.close()
            break
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{TOPIC_NAME}' already exists.")
            break
        except Exception as e:
            logger.info(f"Waiting for Kafka to be ready for topic creation... ({e})")
            time.sleep(5)

async def fetch_weather(session, loc, producer, delay):
    """Fetches weather data after a staggered delay to avoid burst 429s."""
    await asyncio.sleep(delay)

    if MOCK_MODE:
        data = {
            "lat": loc["lat"], "lon": loc["lon"], "location_name": loc["name"],
            "minutely": [{"dt": int(time.time()) + (i * 60), "precipitation": random.random()} for i in range(60)]
        }
    else:
        url = f"https://api.openweathermap.org/data/3.0/onecall?lat={loc['lat']}&lon={loc['lon']}&exclude=current,hourly,daily,alerts&appid={API_KEY}"
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    data["location_name"] = loc["name"]
                elif response.status == 429:
                    logger.error(f"RATE LIMIT HIT (429) for {loc['name']}.")
                    return
                else:
                    logger.error(f"API Error {loc['name']} ({response.status})")
                    return
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return

    # Non-blocking send
    producer.send(TOPIC_NAME, value=data)

async def run_cycle(locations, producer):
    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        # Stagger the 52 requests over 40 seconds to avoid DDOS/ error code 429.
        interval = 40.0 / len(locations) if len(locations) > 0 else 1

        for i, loc in enumerate(locations):
            tasks.append(fetch_weather(session, loc, producer, i * interval))

        await asyncio.gather(*tasks)

async def main():
    if not API_KEY and not MOCK_MODE:
        logger.error("API_KEY missing.")
        return

    locations = load_locations()
    ensure_topic_exists()


    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=10
            )
        except:
            logger.info("Waiting for Kafka Producer connection...")
            await asyncio.sleep(5)

    logger.info(f"Ingestor active. Polling {len(locations)} locations every 60s.")

    while True:
        cycle_start = asyncio.get_event_loop().time()
        await run_cycle(locations, producer)

        elapsed = asyncio.get_event_loop().time() - cycle_start
        sleep_time = max(1, 60 - elapsed)

        logger.info(f"Cycle complete in {elapsed:.2f}s. Sleeping for {sleep_time:.2f}s.")
        await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
