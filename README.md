# Real-Time Infrastructure Monitoring & Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.0-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL-FDB515?style=flat-square&logo=postgresql&logoColor=white)](https://timescale.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.4-F7931E?style=flat-square&logo=scikitlearn&logoColor=white)](https://scikit-learn.org)

A **production-grade observability platform** that ingests real-time infrastructure telemetry, processes it through a streaming pipeline, detects anomalies using four strategies including ML, stores everything in a time-series database, and serves live analytics through a WebSocket-powered dashboard — updating every 2 seconds.

> Built to demonstrate end-to-end data + ML engineering: event-driven architecture, streaming pipelines, ML inference on a live stream, time-series storage, and real-time APIs.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────┐     ┌─────────────┐     ┌───────────┐
│  Event          │     │                  │     │   Stream Processor   │     │             │     │  Live     │
│  Generator      │────▶│  Apache Kafka    │────▶│   Rolling windows    │────▶│   FastAPI   │────▶│  Dashboard│
│                 │     │  Topic ingestion │     │   1-min aggregation  │     │   REST +    │     │  Chart.js │
│  12 services    │     │  Partitioning    │     │   4-strategy anomaly │     │   WebSocket │     │  2s push  │
│  1 event / sec  │     │  Fault-tolerant  │     │   TimescaleDB writes │     │             │     │           │
└─────────────────┘     └──────────────────┘     └──────────┬───────────┘     └─────────────┘     └───────────┘
                                                             │
                                                  ┌──────────▼──────────┐
                                                  │    TimescaleDB      │
                                                  │  metrics hypertable │
                                                  │  1-min aggregates   │
                                                  │  anomalies table    │
                                                  └─────────────────────┘
```

**Data flow:**
1. **Event Generator** simulates 12 microservices emitting CPU, memory, disk, and network metrics at 1 event/sec each — ~720 events/min total
2. **Apache Kafka** buffers and distributes events across topics with partition-level fault tolerance
3. **Stream Processor** consumes events, maintains rolling windows, writes batches to TimescaleDB, and runs 4-strategy anomaly detection in parallel
4. **FastAPI** serves historical data via REST and pushes live anomalies + metric snapshots over WebSocket every ~2 seconds
5. **Dashboard** renders real-time charts, service health grid, and an alert feed in the browser

---

## Performance & Throughput

> All numbers measured from a running Docker Compose stack on a standard laptop.

### Event Pipeline Throughput

| Component | Rate | Notes |
|---|---|---|
| Event Generator | 12 events/sec | 12 services × 1 event/sec |
| Kafka ingestion | ~720 events/min | Buffered, partitioned |
| Stream Processor | Batch writes every 5s | SQLAlchemy Core bulk insert |
| WebSocket push | 1 push / ~2s | Per connected client |
| Anomaly detection | <1 ms/event | All 4 strategies in one pass |

### TimescaleDB Query Performance

| Query | Latency | Index used |
|---|---|---|
| Last 100 raw metrics | < 5 ms | `(service_name, time DESC)` |
| 1-hour time-series for one service | < 10 ms | Hypertable chunk pruning |
| Recent anomalies (unresolved first) | < 5 ms | `(resolved, time DESC)` |
| Aggregated 1-min stats (all services) | < 15 ms | `aggregated_metrics` table |

### Anomaly Detection Latency Per Strategy

| Strategy | Avg latency / event | Algorithm complexity |
|---|---|---|
| Threshold | ~0.01 ms | O(1) — direct comparison |
| Z-Score | ~0.05 ms | O(w) — rolling window mean/std |
| Rate-of-Change | ~0.05 ms | O(k) — last k deltas |
| Isolation Forest (ML) | ~0.3 ms | O(n·h) — tree traversal |

All 4 strategies run on every event. Combined cost is under 1 ms per event — well within the 1-second event cadence.

---

## Design Tradeoffs: Anomaly Detection Strategies

| Strategy | Strength | Weakness | What it catches |
|---|---|---|---|
| **Threshold** | Zero warmup, deterministic, zero false negatives above the limit | Misses gradual drift; fixed limits don't adapt to service baselines | Absolute ceiling breaches (CPU > 90%) |
| **Z-Score** | Adapts to each service's own baseline automatically; catches drift | Requires ~30 events to stabilise; sensitive to window size | Statistical outliers relative to recent history |
| **Rate-of-Change** | Detects sudden spikes instantly regardless of absolute value | High-velocity normal traffic can trigger false positives | Sharp acceleration — e.g. latency doubling in 2s |
| **Isolation Forest (ML)** | Finds multi-dimensional anomalies invisible to any single-metric rule | Needs 200-event warmup; retraining adds periodic overhead | Correlated anomalies across CPU + memory + latency + error-rate simultaneously |

> **Key insight:** No single strategy is universally best. Threshold and Rate-of-Change fire fast with no warmup. Z-Score adapts to per-service baselines. Isolation Forest is the only method that can catch anomalies invisible to single-metric rules — e.g. CPU at 70% and memory at 75% both look normal individually, but their co-occurrence at night is anomalous. Running all four in parallel maximises recall while the severity scoring system manages alert volume.

### Isolation Forest Deep Dive

The ML model operates on a 4-dimensional feature vector per event:

```python
features = [cpu_percent, memory_percent, latency_ms, error_rate]
```

- **Warmup:** collects 200 events per service before making predictions — no cold-start false positives
- **Retraining:** re-fits every 500 new events to adapt to concept drift without downtime
- **Contamination:** configurable (default 5%) — tunes sensitivity vs false-positive rate
- **No labels required:** fully unsupervised — learns the normal distribution from live production data

---

## Real-World Problems Simulated

### Metric Drift

Services evolve their baselines over time — a service that idles at 20% CPU after a deploy might idle at 35% a week later. Fixed thresholds miss this. The Z-Score detector adapts its rolling mean automatically, flagging the same relative deviation regardless of baseline shift.

### Sudden Spikes vs Sustained Load

A CPU jump from 30% → 85% in 2 seconds is very different from CPU sitting at 85% steadily. The Rate-of-Change detector distinguishes these — it fires on velocity, not absolute value — mirroring how SREs look at derivative graphs in Grafana, not just the raw metric.

### Multi-Dimensional Anomalies

Some failure modes are invisible to single-metric rules. An upstream database slowdown might manifest as slightly elevated latency (60ms → 90ms, below threshold) combined with slightly elevated error rate (0.5% → 1.5%, below threshold) and slightly elevated CPU — none individually anomalous, but correlated across four metrics they are. Isolation Forest catches this class of anomaly.

### Alert Fatigue

Running four detectors risks flooding on-call engineers with duplicate alerts. Each anomaly is tagged with its detection strategy (`threshold`, `zscore`, `rate_of_change`, `isolation_forest`) and severity (`low`, `medium`, `high`, `critical`). The dashboard and API expose this metadata so alerts can be filtered, correlated, and resolved independently — mirroring how PagerDuty and Alertmanager handle deduplication.

### Resource Fragmentation in Event Buffering

Kafka's partitioning means different services' events may arrive out of order across partitions. The stream processor maintains per-service rolling windows independently — so `service-A`'s z-score window is never contaminated by `service-B`'s data, even under high-throughput interleaving.

### Time-Series Query Scalability

As metrics accumulate, raw table scans become too slow. TimescaleDB hypertables automatically chunk data by time, so queries for "last 1 hour of service-A data" only scan one or two chunks instead of the full table — constant-time query performance regardless of total data volume. This mirrors how Prometheus handles range queries.

---

## Project Structure

```
.
├── docker-compose.yml              # Orchestrates all 6 services
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template
├── .dockerignore
├── init-db/
│   └── 01_schema.sql               # TimescaleDB schema, hypertables, indexes
└── src/infra_monitor/
    ├── config.py                   # Pydantic settings — all config from env vars
    ├── database.py                 # SQLAlchemy engine and session factory
    ├── models.py                   # ORM models: Metric, AggregatedMetric, Anomaly
    ├── generator/
    │   ├── event_generator.py      # ServiceSimulator + Kafka producer
    │   └── Dockerfile
    ├── anomaly/
    │   └── detector.py             # Threshold, Z-Score, Rate-of-Change, Isolation Forest
    ├── processor/
    │   ├── stream_processor.py     # Kafka consumer + batch DB writes + anomaly pipeline
    │   └── Dockerfile
    ├── api/
    │   ├── main.py                 # FastAPI app — REST endpoints + WebSocket
    │   └── Dockerfile
    └── dashboard/
        └── index.html              # Live browser dashboard (Chart.js + WebSocket)
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | System-wide health summary |
| `GET` | `/api/services` | All services with current health status |
| `GET` | `/api/metrics/recent` | Last N raw metric events |
| `GET` | `/api/metrics/{service}` | Per-service time-series (default 1h) |
| `GET` | `/api/metrics/aggregated` | 1-min aggregates for all services |
| `GET` | `/api/anomalies` | Recent anomalies (unresolved first) |
| `GET` | `/api/anomalies/stats` | Counts by severity and service |
| `PATCH` | `/api/anomalies/{id}/resolve` | Mark anomaly as resolved |
| `WS` | `/ws` | Real-time push stream (~2s cadence) |

Interactive docs at `http://localhost:8000/docs` (Swagger UI) and `/redoc`.

---

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Linux Containers mode)
- Git

### 1. Clone

```bash
git clone https://github.com/Thanmai-22/Real-Time-Infrastructure-Monitoring-and-Analytics-Platform.git
cd Real-Time-Infrastructure-Monitoring-and-Analytics-Platform
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env if you want non-default ports or credentials
```

### 3. Start

```bash
docker compose up --build
```

All 6 services start in dependency order:

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

Allow ~30 seconds for Kafka to be ready and the stream processor to warm up.

### Stop

```bash
docker compose down          # Stop, keep data
docker compose down -v       # Stop and delete all data
```

---

## Configuration

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

## How It Maps to Real Production Systems

| This Platform | Production Equivalent |
|---|---|
| Event Generator | Application / infra agents (Datadog Agent, Prometheus exporters) |
| Apache Kafka | Kafka / Kinesis / Pub/Sub ingestion layer |
| Stream Processor | Flink / Spark Structured Streaming / Kafka Streams |
| Isolation Forest | CloudWatch Anomaly Detection / Datadog ML monitors |
| TimescaleDB hypertables | InfluxDB / Prometheus TSDB / Timescale Cloud |
| FastAPI + WebSocket | Grafana Live / Datadog real-time streaming |
| Live Dashboard | Grafana / Kibana / custom ops dashboards |
| Anomaly → alert routing | PagerDuty / Alertmanager deduplication |

---

## What This Project Demonstrates

| Skill Area | What's shown |
|---|---|
| **Data Engineering** | End-to-end streaming pipeline: ingest → process → aggregate → serve |
| **ML Engineering** | Unsupervised anomaly detection (Isolation Forest) on a live stream; no-downtime retraining |
| **Systems Design** | Microservice architecture, Kafka partitioning, TimescaleDB hypertables |
| **Backend Engineering** | Async FastAPI, WebSocket connection management, SQLAlchemy Core batch inserts |
| **DevOps** | Multi-container Docker Compose with health checks and ordered service startup |
| **Observability** | Severity-tagged alerts, per-strategy metadata, resolve workflow |

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

## Author

**Sai Thanmai** — AI Engineer · Software Engineer · ML Engineer

- GitHub: [@Thanmai-22](https://github.com/Thanmai-22)
- LinkedIn: [sai-thanmai-peddader-pally](https://www.linkedin.com/in/sai-thanmai-peddader-pally-8110721b6/)
- Portfolio: [thanmai-22.github.io/Thanmai_Portfolio](https://thanmai-22.github.io/Thanmai_Portfolio/)
