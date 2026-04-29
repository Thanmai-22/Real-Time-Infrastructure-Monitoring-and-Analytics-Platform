"""
Stream Processor
────────────────
Consumes raw metric events from Kafka, then:

  1. Writes raw rows to the `metrics` TimescaleDB hypertable.
  2. Maintains per-service rolling buffers → computes 1-min aggregates
     and flushes them to `aggregated_metrics` every FLUSH_INTERVAL events.
  3. Runs each event through the AnomalyDetector; persists anomalies to DB
     and forwards them to the `infrastructure.anomalies` Kafka topic.

Throughput notes:
  - Batch-inserts up to BATCH_SIZE rows at once for raw metrics.
  - Uses SQLAlchemy Core (not ORM) for bulk inserts — much faster.
"""

import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import text

from infra_monitor.anomaly.detector import AnomalyDetector, AnomalyEvent
from infra_monitor.database import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [processor] %(message)s")
log = logging.getLogger(__name__)

BATCH_SIZE    = 50     # flush raw metrics every N events
FLUSH_INTERVAL = 60    # flush aggregated metrics every N events per service
WINDOW_SECONDS = 60    # rolling window for aggregation


# ── Rolling window buffers ─────────────────────────────────────────────────────
class MetricBuffer:
    """Holds the last WINDOW_SECONDS seconds of metric values for a service."""

    def __init__(self):
        self.cpu:     deque[float] = deque(maxlen=WINDOW_SECONDS)
        self.memory:  deque[float] = deque(maxlen=WINDOW_SECONDS)
        self.latency: deque[float] = deque(maxlen=WINDOW_SECONDS)
        self.requests: deque[int]  = deque(maxlen=WINDOW_SECONDS)
        self.errors:  deque[float] = deque(maxlen=WINDOW_SECONDS)
        self.count    = 0

    def push(self, event: dict):
        self.cpu.append(event["cpu_percent"])
        self.memory.append(event["memory_percent"])
        self.latency.append(event["latency_ms"])
        self.requests.append(event["request_count"])
        self.errors.append(event["error_rate"])
        self.count += 1

    def aggregate(self, service: str) -> dict:
        lat = list(self.latency)
        return {
            "time":           datetime.now(timezone.utc).isoformat(),
            "service":        service,
            "window_seconds": WINDOW_SECONDS,
            "avg_cpu":        float(np.mean(self.cpu)) if self.cpu else None,
            "max_cpu":        float(np.max(self.cpu))  if self.cpu else None,
            "avg_memory":     float(np.mean(self.memory)) if self.memory else None,
            "max_memory":     float(np.max(self.memory))  if self.memory else None,
            "avg_latency_ms": float(np.mean(lat)) if lat else None,
            "p95_latency_ms": float(np.percentile(lat, 95)) if len(lat) >= 2 else None,
            "max_latency_ms": float(np.max(lat)) if lat else None,
            "total_requests": int(sum(self.requests)),
            "avg_error_rate": float(np.mean(self.errors)) if self.errors else None,
            "sample_count":   self.count,
        }


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def build_consumer(bootstrap: str, topic: str, group: str) -> KafkaConsumer:
    for attempt in range(1, 13):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                group_id=group,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            log.info("Consumer connected to Kafka %s, topic=%s", bootstrap, topic)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/12 in 5s …", attempt)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka consumer")


def build_producer(bootstrap: str) -> KafkaProducer:
    for attempt in range(1, 11):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                linger_ms=100,
            )
        except NoBrokersAvailable:
            log.warning("Kafka producer not ready, retry %d/10 …", attempt)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka producer")


# ── DB helpers ────────────────────────────────────────────────────────────────
RAW_INSERT = text("""
    INSERT INTO metrics
        (time, service, node, cpu_percent, memory_percent, latency_ms, request_count, error_rate)
    VALUES
        (:time, :service, :node, :cpu_percent, :memory_percent, :latency_ms, :request_count, :error_rate)
    ON CONFLICT DO NOTHING
""")

AGG_INSERT = text("""
    INSERT INTO aggregated_metrics
        (time, service, window_seconds, avg_cpu, max_cpu, avg_memory, max_memory,
         avg_latency_ms, p95_latency_ms, max_latency_ms, total_requests, avg_error_rate, sample_count)
    VALUES
        (:time, :service, :window_seconds, :avg_cpu, :max_cpu, :avg_memory, :max_memory,
         :avg_latency_ms, :p95_latency_ms, :max_latency_ms, :total_requests, :avg_error_rate, :sample_count)
    ON CONFLICT DO NOTHING
""")

ANOMALY_INSERT = text("""
    INSERT INTO anomalies
        (time, service, node, metric, value, threshold, z_score, severity, method)
    VALUES
        (:time, :service, :node, :metric, :value, :threshold, :z_score, :severity, :method)
""")


def flush_raw(batch: list[dict]):
    with engine.begin() as conn:
        conn.execute(RAW_INSERT, batch)


def flush_aggregated(rows: list[dict]):
    with engine.begin() as conn:
        conn.execute(AGG_INSERT, rows)


def persist_anomalies(anomalies: list[AnomalyEvent]):
    rows = [
        {
            "time": a.time, "service": a.service, "node": a.node,
            "metric": a.metric, "value": a.value,
            "threshold": a.threshold, "z_score": a.z_score,
            "severity": a.severity, "method": a.method,
        }
        for a in anomalies
    ]
    with engine.begin() as conn:
        conn.execute(ANOMALY_INSERT, rows)


# ── Main processing loop ──────────────────────────────────────────────────────
def main():
    bootstrap     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_metrics = os.getenv("KAFKA_TOPIC_METRICS", "infrastructure.metrics")
    topic_anomaly = os.getenv("KAFKA_TOPIC_ANOMALIES", "infrastructure.anomalies")
    group         = os.getenv("KAFKA_CONSUMER_GROUP", "infra-processor")

    consumer  = build_consumer(bootstrap, topic_metrics, group)
    producer  = build_producer(bootstrap)
    detector  = AnomalyDetector()
    buffers: dict[str, MetricBuffer] = defaultdict(MetricBuffer)

    raw_batch: list[dict]  = []
    agg_rows:  list[dict]  = []
    total = 0

    log.info("Stream processor running …")

    while True:
        for msg in consumer:
            event: dict = msg.value
            total += 1

            service = event.get("service", "unknown")
            buffers[service].push(event)

            # Accumulate raw batch
            raw_batch.append({
                "time":           event.get("timestamp"),
                "service":        service,
                "node":           event.get("node", "unknown"),
                "cpu_percent":    event.get("cpu_percent"),
                "memory_percent": event.get("memory_percent"),
                "latency_ms":     event.get("latency_ms"),
                "request_count":  event.get("request_count"),
                "error_rate":     event.get("error_rate"),
            })

            # Flush raw batch
            if len(raw_batch) >= BATCH_SIZE:
                try:
                    flush_raw(raw_batch)
                except Exception as exc:
                    log.error("Raw insert failed: %s", exc)
                raw_batch.clear()

            # Flush per-service aggregations
            buf = buffers[service]
            if buf.count % FLUSH_INTERVAL == 0:
                agg = buf.aggregate(service)
                agg_rows.append(agg)
                if len(agg_rows) >= 10:
                    try:
                        flush_aggregated(agg_rows)
                    except Exception as exc:
                        log.error("Aggregated insert failed: %s", exc)
                    agg_rows.clear()

            # Anomaly detection
            anomalies = detector.analyze(event)
            if anomalies:
                try:
                    persist_anomalies(anomalies)
                except Exception as exc:
                    log.error("Anomaly insert failed: %s", exc)

                for a in anomalies:
                    producer.send(topic_anomaly, {
                        "time": a.time, "service": a.service, "node": a.node,
                        "metric": a.metric, "value": a.value,
                        "severity": a.severity, "method": a.method,
                        "z_score": a.z_score, "threshold": a.threshold,
                    })
                    log.warning(
                        "[%s] %s anomaly on %s — value=%.2f (method=%s)",
                        a.severity.upper(), a.service, a.metric, a.value, a.method,
                    )

            if total % 500 == 0:
                log.info("Processed %d events total", total)

        # Short sleep when consumer has no messages
        time.sleep(0.1)


if __name__ == "__main__":
    main()
