# Weather Precipitation Stream Processor

## Overview

This application performs real-time processing of minute-by-minute precipitation forecasts. It is designed to scale to thousands of coordinate pairs by utilizing a decoupled microservice architecture.

### Tech Stack

- Language: Python 3.11

- Message Queue: Apache Kafka (Scalable, fault-tolerant ingestion)

- Stream Processing: PySpark (Structured Streaming for windowed aggregations)

- Storage: PostgreSQL (Structured storage for processed results)

- Orchestration: Docker Compose

### Architecture & Design

- Producer (ingestor/): Simulates a stream by polling the API every minute. It maps each coordinate pair to a Kafka partition to ensure order and scalability.

- Kafka: Acts as the buffer. Even if the processor is down, Kafka retains the data (fault tolerance).

- Spark Processor (processor/): Consumes the JSON stream. It performs a sum of the precipitation values within the minutely array.

- Database: Stores the latest "Next Hour Total" for each coordinate.

### Scalability Strategy

- Horizontal Scaling: Kafka partitions allow multiple consumer instances. Spark can be deployed on a cluster (YARN/Kubernetes) to handle tens of thousands of coordinates.

- Backpressure: Spark Structured Streaming naturally handles spikes in data volume by processing in micro-batches.

## Setup & Execution

### Prerequisites

- Docker and Docker Compose

- OpenWeatherMap API Key (A default is provided in the code, but replace if needed)

### Instructions

#  Clone the repository (or unzip).

Build and Start:

```bash
docker-compose up --build
```

View Results:
The processor writes to the weather_stats table in Postgres. You can check the output:

```bash
docker exec -it weather-db psql -U sensorfact -d weather_db -c "SELECT * FROM precipitation_forecasts;"
```

#### Option B: Live Terminal Monitor (Recommended for Demo)
Run the provided monitor script locally (requires psycopg2):

```bash
python monitor.py
```

This script will auto-refresh every 5 seconds, displaying a formatted table of all active coordinate pairs and their forecasted rain volume.

## Development Decisions

- Static Mocking: To prevent hitting the 1,000 call daily limit during testing, the ingestor has a MOCK_MODE environment variable.

- Checkpointing: Spark uses a checkpoint directory to ensure that if the process restarts, it picks up exactly where it left off without losing data.