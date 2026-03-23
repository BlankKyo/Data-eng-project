# ✈️ Sky-Watcher: Real-Time Aviation Streaming Pipeline

A high-performance, **event-driven ETL pipeline** designed to ingest, process, and persist global aircraft movements in France region. This project demonstrates a professional transition from legacy batch processing to a **Modern Data Stack** utilizing Kafka and Time-Series databases.

---

## 🏗️ System Architecture

The project follows a Decoupled **Medallion Architecture** to ensure high throughput and data integrity:

1.  **Gold (Analytics/Persistence):** Python-based extraction of state vectors for France geographic bounding box.
2.  **Bronze (Stream):** The `OpenSkyProducer` fetches live state vectors via the OpenSky REST API and streams them into **Apache Kafka** as raw JSON.
3.  **Silver (Persistence):** The `FlightConsumer` subscribes to Kafka, performs micro-batching (10K rows), schema mapping, coordinate validation and Atomic UPSERTs into PostgreSQL.

---

## 📂 Project Structure

DATA_ENG_PROJ/
├── app/
│   ├── main.py          # Orchestrator: Multi-threaded Producer/Consumer management
│   ├── core/            # The "Logic" Layer (Python)
│   │   ├── producer.py  # Kafka Producer: Async Kafka streaming of raw API snapshots
│   │   ├── consumer.py  # Kafka Consumer: Micro-batching Kafka messages into Atomic DB transactions
│   │   ├── transform.py # Data Cleaning: Schema mapping & Coordinate validation
│   │   └── extract.py   # API Utilities: Bounding Box & OpenSky REST calls
│   └── db/              # The "Persistence" Layer (SQL & Python)
│       ├── queries/     # SQL Scripts: Table schemas, Hypertable configs
│       ├── database.py  # Schema Orchestrator: Connects to DB and runs SQL files
│       └── load.py      # Batch Loader: High-speed inserts & data lifecycle (Cleanup)
├── docker-compose.yaml  # Infrastructure: Kafka, Zookeeper, TimescaleDB, AKHQ, pgAdmin
├── Dockerfile           # Deployment: Multi-stage Python build for the tracker service
├── .env.app             # App Config: API URLs and keys
├── .env.docker          # Infra Config: DB Credentials & Broker settings (Hidden by Git)
└── .gitignore           # Safety: Filters for secrets, __pycache__, and local logs


## 🚀 Getting Started

### 1. 🛠️ Prerequisites
- Docker & Docker Compose (Required)
- OpenSky Network Account (Recommended for higher API limits)

### 2. 🔑 Environment Configuration
Create two .env files in the root directory.
You can copy them from .env.example files:

cp .env.app.example .env.app
cp .env.docker.example .env.docker

#### .env.app
```
DB_HOST=db
DB_NAME=aviation_db
DB_USER=admin
DB_PASS=supersecret

```

#### .env.docker
```
POSTGRES_USER=admin
POSTGRES_PASSWORD=supersecret
POSTGRES_DB=aviation_db

PGADMIN_EMAIL = admin@pro.com
PGADMIN_PASSWORD = admin

```

### 3. 🐳 Run the Full Pipeline
```
Build and start all services (Kafka, TimescaleDB, Producer, Consumer, pgAdmin, AKHQ):

docker-compose up -d --build

This will start:

. PostgreSQL / TimescaleDB
. Kafka & Zookeeper
. Kafka UI (AKHQ)
. pgAdmin
. Flight tracker Producer/Consumer service
```

## 🛠️ Key Engineering Features
- **Batch Processing:** Uses execute_batch to reduce database I/O overhead by 10x.
- **Transactional Integrity:** Implements Commit/Rollback logic to ensure metadata and telemetry stay synchronized.
- **Data Retention:** Automatically purges flight telemetry older than 1 hour to keep storage lean and performant.
