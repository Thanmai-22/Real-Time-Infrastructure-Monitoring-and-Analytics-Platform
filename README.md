# Real-Time Infrastructure Monitoring & Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.0-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL-FDB515?style=flat-square&logo=postgresql&logoColor=white)](https://timescale.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.4-F7931E?style=flat-square&logo=scikitlearn&logoColor=white)](https://scikit-learn.org)

A **production-grade observability platform** that ingests real-time infrastructure telemetry, processes it through a streaming pipeline, detects anomalies using four strategies (including ML), stores everything in a time-series database, and serves live analytics through a WebSocket-powered dashboard.

> Built to demonstrate end-to-end data + ML engineering: event-driven architecture, streaming pipelines, ML inference at scale, time-series storage, and real-time APIs.

---

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐     ┌─────────────┐     ┌───────────┐
│  Event          │     │              │     │  Stream Processor   │     │             │     │  Live     │
│  Generator      │────▶│  Apache      │────▶│  ─ Rolling stats    │────▶│  FastAPI    │────▶│  Dashboard│
│                 │     │  Kafka       │     │  ─ Aggregation      │     │  REST +     │     │  Chart.js │
│  12 services    │     │  (ingestion) │     │  ─ Anomaly detect   │     │  WebSocket  │     │  2s push  │
│  1 event/sec    │     │              │     │  ─ DB writes        │     │             │     │           │
└─────────────────┘     └──────────────┘     └──────────┬──────────┘     └─────────────┘     └───────────┘
                                                         │
                                                         ▼
                                               ┌──────────────────┐
                                               │  TimescaleDB     │
                                               │  ─ metrics       │
                                               │  ─ aggregates    │
                                               │  ─ anomalies     │
                                               └──────────────────┘
```

**Data Flow:**

1. **Event Generator** simulates 12 microservices emitting CPU, memory, disk, and network metrics at 1 event/sec each (~720 events/min)
2. **Apache Kafka** buffers and distributes events across topics with partition-level fault tolerance
3. **Stream Processor** consumes events, maintains rolling windows, writes batches to TimescaleDB, and runs 4-strategy anomaly detection
4. **FastAPI** serves historical data via REST and pushes live anomalies + metric snapshots over WebSocket every ~2 seconds
5. **Dashboard** renders real-time charts, service health, and alerts in the browser

---

## Features

### Streaming Pipeline
- 12 simulated services generating realistic telemetry with random-walk evolution and randomised spike injection
- Apache Kafka ingestion with configurable topic partitioning and consumer groups
- Batch writes to TimescaleDB with rolling 1-minute continuous aggregates

### 4-Strategy Anomaly Detection
| Strategy | Method | What it catches |
|---|---|---|
| **Threshold** | Static limits per metric | Absolute ceiling breaches (CPU > 90%) |
| **Z-Score** | Rolling mean ± N·σ | Statistical outliers vs recent history |
| **Rate-of-Change** | Velocity vs recent average | Sudden spikes regardless of absolute value |
| **Isolation Forest** | scikit-learn ML model | Multi-dimensional anomalies invisible to rules |

The ML model warms up on the first 200 events per service and retrains every 500 events, requiring no labelled data.

### Time-Series Storage
- TimescaleDB hypertables for `metrics` and `aggregated_metrics` with automatic chunk management
- 1-minute continuous aggregates (avg, p95, max) materialised by the stream processor
- Indexed on `(service_name, time DESC)` for sub-millisecond queries

### REST API + WebSocket
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | System health summary |
| `GET` | `/api/services` | All services with current health status |
| `GET` | `/api/metrics/recent` | Last N raw metric events |
| `GET` | `/api/metrics/{service}` | Per-service time-series (default 1h) |
| `GET` | `/api/metrics/aggregated` | 1-min aggregates for all services |
| `GET` | `/api/anomalies` | Recent anomalies (unresolved first) |
| `GET` | `/api/anomalies/stats` | Counts by severity and service |
| `PATCH` | `/api/anomalies/{id}/resolve` | Mark anomaly as resolved |
| `WS` | `/ws` | Real-time push stream (~2s cadence) |

### Live Dashboard
- Real-time CPU / memory / latency charts via Chart.js
- Service health grid with colour-coded status
- Alert feed with severity badges and 🤖 ML indicator for Isolation Forest detections
- Auto-reconnecting WebSocket client

---

## Project Structure

```
.
├── docker-compose.yml              # Orchestrates all 6 services
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template
├── .dockerignore
├── init-db/
│   └── 01_schema.sql               # TimescaleDB schema & hypertables
└── src/infra_monitor/
    ├── config.py                   # Pydantic settings (env-driven)
    ├── database.py                 # SQLAlchemy engine & session
    ├── models.py                   # ORM models: Metric, AggregatedMetric, Anomaly
    ├── generator/
    │   ├── event_generator.py      # ServiceSimulator + Kafka producer
    │   └── Dockerfile
    ├── anomaly/
    │   └── detector.py             # Threshold, Z-Score, Rate-of-Change, Isolation Forest
    ├── processor/
    │   ├── stream_processor.py     # Kafka consumer + DB writes + anomaly orchestration
    │   └── Dockerfile
    ├── api/
    │   ├── main.py                 # FastAPI app — REST + WebSocket
    │   └── Dockerfile
    └── dashboard/
        └── index.html              # Live browser dashboard
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Linux Containers mode)
- Git

### 1. Clone the repository

```bash
git clone https://github.com/Thanmai-22/Real-Time-Infrastructure-Monitoring-and-Analytics-Platform.git
cd Real-Time-Infrastructure-Monitoring-and-Analytics-Platform
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env if you want non-default ports or credentials
```

### 3. Start the platform

```bash
docker compose up --build
```

This single command starts all 6 services in dependency order:

| Service | Role | Port |
|---|---|---|
| `zookeeper` | Kafka coordination | 2181 |
| `kafka` | Event streaming | 9092 |
| `timescaledb` | Time-series database | 5432 |
| `generator` | Metric simulation | — |
| `processor` | Stream processing + ML | — |
| `api` | REST + WebSocket backend | **8000** |

### 4. Open the dashboard

```
http://localhost:8000
```

Allow ~30 seconds for Kafka to be ready and the stream processor to warm up before the first metrics appear.

### 5. Explore the API

Interactive API docs are available at:

```
http://localhost:8000/docs       # Swagger UI
http://localhost:8000/redoc      # ReDoc
```

---

## Configuration

All configuration is managed via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `DATABASE_URL` | `postgresql://...` | TimescaleDB connection string |
| `METRICS_TOPIC` | `infra.metrics` | Kafka topic for raw metrics |
| `ANOMALIES_TOPIC` | `infra.anomalies` | Kafka topic for detected anomalies |
| `ANOMALY_ZSCORE_THRESHOLD` | `3.0` | Z-score sensitivity |
| `ANOMALY_ROC_THRESHOLD` | `0.5` | Rate-of-change sensitivity |
| `IF_CONTAMINATION` | `0.05` | Isolation Forest contamination rate |

---

## Anomaly Detection Deep Dive

### Isolation Forest (ML)

The Isolation Forest model runs per-service and operates on a 4-dimensional feature vector:

```python
features = [cpu_percent, memory_percent, latency_ms, error_rate]
```

- **Warmup:** collects 200 events before making predictions
- **Retraining:** re-fits every 500 new events to adapt to drift
- **Contamination:** configurable (default 5%) — tunes sensitivity vs false-positive rate
- **No labels required:** fully unsupervised; learns the normal distribution from live data

### Z-Score Detection

Maintains a rolling window of the last 100 observations per (service, metric) pair. Flags readings that deviate more than `ANOMALY_ZSCORE_THRESHOLD` standard deviations from the rolling mean.

### Rate-of-Change Detection

Computes the velocity of the last 5 readings against the rolling average velocity. Flags sudden spikes where the current delta exceeds `ANOMALY_ROC_THRESHOLD × mean_velocity`.

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Python 3.11 |
| Streaming | Apache Kafka, kafka-python |
| Storage | TimescaleDB (PostgreSQL), SQLAlchemy |
| ML | scikit-learn (Isolation Forest), NumPy, SciPy |
| API | FastAPI, Uvicorn, WebSockets |
| Dashboard | HTML, JavaScript, Chart.js |
| Infrastructure | Docker Compose |
| Config | Pydantic Settings, python-dotenv |

---

## Stopping the Platform

```bash
docker compose down          # Stop containers, keep volumes (data persists)
docker compose down -v       # Stop containers AND delete all data
```

---

## What This Project Demonstrates

| Skill Area | What's shown |
|---|---|
| **Data Engineering** | End-to-end streaming pipeline: ingest → process → store → serve |
| **ML Engineering** | Unsupervised anomaly detection in a live stream; model retraining without downtime |
| **Systems Design** | Microservice architecture, Kafka partitioning, time-series hypertables |
| **Backend Engineering** | Async FastAPI, WebSocket connection management, SQLAlchemy Core batch inserts |
| **DevOps** | Multi-container Docker Compose with health checks and dependency ordering |

---

## Author

**Sai Thanmai** — AI Engineer · Software Engineer · ML Engineer

- GitHub: [@Thanmai-22](https://github.com/Thanmai-22)
- LinkedIn: [sai-thanmai-peddader-pally](https://www.linkedin.com/in/sai-thanmai-peddader-pally-8110721b6/)
- Portfolio: [thanmai-22.github.io/Thanmai_Portfolio](https://thanmai-22.github.io/Thanmai_Portfolio/)
