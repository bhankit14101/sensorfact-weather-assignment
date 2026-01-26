import os
import asyncio
import json
import logging
import csv
import time
from aiohttp import ClientSession, ClientTimeout
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestor")

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
API_KEY = os.getenv("OPENWEATHER_API_KEY")
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"
TOPIC_NAME = "weather_raw"
LOCATIONS_FILE = "locations.csv"
RATE_LIMIT_PER_MIN = 60  # OpenWeather Free Tier limit

def load_locations():
    locations = []
    try:
        with open(LOCATIONS_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                locations.append({
                    "name": row['name'],
                    "lat": float(row['lat']),
                    "lon": float(row['lon'])
                })
        logger.info(f"Loaded {len(locations)} locations.")
    except Exception as e:
        logger.error(f"Failed to load locations: {e}")
        locations = [{"name": "Default Utrecht", "lat": 52.0845, "lon": 5.1155}]
    return locations

def ensure_topic_exists():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, client_id='weather-admin')
        topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(f"Topic '{TOPIC_NAME}' verified.")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logger.error(f"AdminClient error: {e}")

async def fetch_weather(session, loc, semaphore, producer):
    """Fetches weather data for a single location using a semaphore for rate limiting."""
    async with semaphore:
        if MOCK_MODE:
            data = {
                "lat": loc["lat"], "lon": loc["lon"], "location_name": loc["name"],
                "minutely": [{"dt": int(time.time()) + (i * 60), "precipitation": 0.1} for i in range(60)]
            }
        else:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={loc['lat']}&lon={loc['lon']}&exclude=current,hourly,daily,alerts&appid={API_KEY}"
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        data["location_name"] = loc["name"]
                    else:
                        text = await response.text()
                        logger.error(f"API Error for {loc['name']} ({response.status}): {text}")
                        return
            except Exception as e:
                logger.error(f"Request failed for {loc['name']}: {e}")
                return

        # Produce to Kafka
        try:
            producer.send(TOPIC_NAME, value=data)
        except Exception as e:
            logger.error(f"Kafka produce error: {e}")

async def run_cycle(locations, producer):
    """Executes one full cycle of API polling for all locations."""
    # The Semaphore ensures we only have 60 active requests in flight/minute
    # This is a basic implementation; for 10k locations, staggered scheduling is needed.
    semaphore = asyncio.Semaphore(RATE_LIMIT_PER_MIN)
    timeout = ClientTimeout(total=10)

    async with ClientSession(timeout=timeout) as session:
        tasks = [fetch_weather(session, loc, semaphore, producer) for loc in locations]
        await asyncio.gather(*tasks)

async def main():
    if not API_KEY and not MOCK_MODE:
        logger.error("CRITICAL: API_KEY not found!")
        return

    locations = load_locations()
    ensure_topic_exists()

    # Initialize Producer (Sync library used within Async loop is okay here
    # as producer.send() is non-blocking/buffered)
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,
                linger_ms=50 # Better throughput
            )
        except:
            logger.info("Waiting for Kafka...")
            await asyncio.sleep(5)

    logger.info(f"Async Ingestor started. Locations: {len(locations)}")

    while True:
        start_time = asyncio.get_event_loop().time()

        await run_cycle(locations, producer)

        elapsed = asyncio.get_event_loop().time() - start_time
        sleep_time = max(0, 60 - elapsed)

        logger.info(f"Cycle finished in {elapsed:.2f}s. Sleeping for {sleep_time:.2f}s.")
        await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass