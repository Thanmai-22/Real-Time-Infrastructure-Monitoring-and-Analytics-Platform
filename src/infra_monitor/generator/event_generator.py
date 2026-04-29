"""
Event Generator
───────────────
Simulates infrastructure telemetry for a fleet of services and nodes.
Each service follows a random-walk baseline with configurable spike injection.
Events are published to Kafka as JSON.

Simulated metrics per event:
  - cpu_percent      : 0–100 %
  - memory_percent   : 0–100 %
  - latency_ms       : request latency in milliseconds
  - request_count    : requests handled in this interval
  - error_rate       : error percentage 0–100
"""

import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [generator] %(message)s")
log = logging.getLogger(__name__)

# ── Topology ──────────────────────────────────────────────────────────────────
SERVICES = [
    "api-gateway",
    "auth-service",
    "user-service",
    "payment-service",
    "notification-service",
    "inventory-service",
    "search-service",
    "analytics-service",
    "cache-service",
    "db-proxy",
    "order-service",
    "recommendation-engine",
]

NODES = ["node-01", "node-02", "node-03", "node-04", "node-05"]

# Normal operating baselines per metric: (mean, std, min, max)
BASELINES = {
    "cpu_percent":    (42.0, 8.0,   0.0, 100.0),
    "memory_percent": (58.0, 6.0,  10.0, 100.0),
    "latency_ms":    (180.0, 35.0,  5.0, 8000.0),
    "request_count": (310.0, 50.0,  0.0, 2000.0),
    "error_rate":     (1.8,  0.6,   0.0,  100.0),
}

# Spike profiles injected randomly
SPIKES = [
    # (metric, spike_value_range, probability_per_tick)
    ("cpu_percent",    (88.0, 99.0),  0.02),
    ("latency_ms",     (2500.0, 7000.0), 0.015),
    ("error_rate",     (20.0, 60.0),  0.012),
    ("memory_percent", (91.0, 99.0),  0.008),
]


class ServiceSimulator:
    """Maintains per-service state and produces realistic metric evolution."""

    def __init__(self, name: str):
        self.name = name
        self.node = random.choice(NODES)
        self._state: dict[str, float] = {
            k: random.gauss(mean, std)
            for k, (mean, std, lo, hi) in BASELINES.items()
        }
        self._clamp()
        # Inject slow memory leak trend to ~10% of services
        self._memory_leak = random.random() < 0.1
        self._tick = 0

    def _clamp(self):
        for k, (_, _, lo, hi) in BASELINES.items():
            self._state[k] = max(lo, min(hi, self._state[k]))

    def step(self) -> dict:
        self._tick += 1

        # Occasionally reassign node (simulates migration / restart)
        if random.random() < 0.002:
            self.node = random.choice(NODES)

        # Random walk with mean reversion
        for key, (mean, std, lo, hi) in BASELINES.items():
            drift = 0.0
            if key == "memory_percent" and self._memory_leak:
                drift = 0.05 * math.sin(self._tick / 200) + 0.01
            noise = random.gauss(0, std * 0.15)
            reversion = 0.08 * (mean - self._state[key])
            self._state[key] += noise + reversion + drift

        # Spike injection
        for metric, (lo, hi), prob in SPIKES:
            if random.random() < prob:
                self._state[metric] = random.uniform(lo, hi)

        self._clamp()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.name,
            "node": self.node,
            "cpu_percent":    round(self._state["cpu_percent"], 2),
            "memory_percent": round(self._state["memory_percent"], 2),
            "latency_ms":     round(self._state["latency_ms"], 2),
            "request_count":  max(0, int(self._state["request_count"])),
            "error_rate":     round(self._state["error_rate"], 2),
        }


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=50,
            )
            log.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/10 in 5s …", attempt)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic     = os.getenv("KAFKA_TOPIC_METRICS", "infrastructure.metrics")
    interval  = float(os.getenv("EMIT_INTERVAL_SECONDS", "1"))

    producer   = build_producer(bootstrap)
    simulators = {svc: ServiceSimulator(svc) for svc in SERVICES}

    log.info("Event generator started — emitting to topic '%s' every %.1fs", topic, interval)

    tick = 0
    while True:
        tick += 1
        for svc, sim in simulators.items():
            event = sim.step()
            producer.send(topic, value=event)

        producer.flush()

        if tick % 60 == 0:
            log.info("Tick %d — %d events/sec emitted", tick, len(SERVICES))

        time.sleep(interval)


if __name__ == "__main__":
    main()
