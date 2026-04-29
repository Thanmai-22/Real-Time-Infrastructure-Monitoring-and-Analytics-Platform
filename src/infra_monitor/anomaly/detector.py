"""
Anomaly Detector
────────────────
Four complementary detection strategies:

1. Threshold      — instant flag when a metric crosses a hard limit.
2. Z-Score        — statistical: value is an outlier vs. recent rolling history.
3. Rate-of-Change — sudden delta spike compared to recent change velocity.
4. Isolation Forest (ML) — unsupervised model that learns what "normal" looks
                   like per service/metric and flags deviations automatically.
                   Retrains every RETRAIN_EVERY samples on a rolling window.

Each strategy emits a structured AnomalyEvent that the stream processor
persists to the database and forwards to the Kafka anomalies topic.
"""

import logging
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

import numpy as np
from sklearn.ensemble import IsolationForest

from infra_monitor.config import settings

log = logging.getLogger(__name__)

# ── Types ─────────────────────────────────────────────────────────────────────
Severity = Literal["warning", "critical"]
Method   = Literal["threshold", "zscore", "rate_of_change", "isolation_forest"]

MONITORED_METRICS = [
    "cpu_percent",
    "memory_percent",
    "latency_ms",
    "error_rate",
]

# Threshold definitions: (warning_level, critical_level)
THRESHOLDS: dict[str, tuple[float, float]] = {
    "cpu_percent":    (75.0, settings.anomaly_cpu_threshold),
    "memory_percent": (80.0, settings.anomaly_memory_threshold),
    "latency_ms":     (1000.0, settings.anomaly_latency_threshold),
    "error_rate":     (8.0, settings.anomaly_error_rate_threshold),
}


@dataclass
class AnomalyEvent:
    time:      str
    service:   str
    node:      str
    metric:    str
    value:     float
    threshold: float | None
    z_score:   float | None
    severity:  Severity
    method:    Method


# ── Rolling statistics helper ──────────────────────────────────────────────────
class RollingStats:
    """Lightweight online mean/variance for a fixed-size sliding window."""

    def __init__(self, maxlen: int = 60):
        self._window: deque[float] = deque(maxlen=maxlen)

    def push(self, value: float):
        self._window.append(value)

    @property
    def count(self) -> int:
        return len(self._window)

    @property
    def mean(self) -> float:
        if not self._window:
            return 0.0
        return sum(self._window) / len(self._window)

    @property
    def std(self) -> float:
        if len(self._window) < 2:
            return 0.0
        m = self.mean
        variance = sum((x - m) ** 2 for x in self._window) / len(self._window)
        return math.sqrt(variance)

    def z_score(self, value: float) -> float | None:
        if len(self._window) < 10:
            return None
        s = self.std
        if s < 1e-9:
            return None
        return (value - self.mean) / s


# ── Isolation Forest per-series detector ──────────────────────────────────────
class IsolationForestDetector:
    """
    Maintains one IsolationForest model per (service, metric) pair.

    How it works:
      - Collects a rolling buffer of recent values (up to WINDOW samples).
      - Once WARMUP samples are collected it trains the first model.
      - Retrains every RETRAIN_EVERY new samples so the model adapts to
        gradual baseline shifts (e.g. a service that gets busier over time).
      - For each new value it asks: "does my trained model consider this
        an outlier?" — if yes, emit an isolation_forest anomaly.

    contamination=0.04 tells the model to expect ~4% of training points
    to be anomalies (matching our generator's spike injection probability).
    """

    WARMUP        = 60    # samples before first prediction
    RETRAIN_EVERY = 30    # retrain every N new samples after warmup
    WINDOW        = 300   # rolling buffer size (training data)
    CONTAMINATION = 0.04  # expected fraction of anomalies in training data

    def __init__(self):
        self._buffers: dict[tuple, deque] = defaultdict(
            lambda: deque(maxlen=self.WINDOW)
        )
        self._models:        dict[tuple, IsolationForest] = {}
        self._since_retrain: dict[tuple, int] = defaultdict(int)

    def _retrain(self, key: tuple):
        buf = self._buffers[key]
        X   = np.array(buf).reshape(-1, 1)
        model = IsolationForest(
            n_estimators=100,
            contamination=self.CONTAMINATION,
            random_state=42,
            warm_start=False,
        )
        model.fit(X)
        self._models[key] = model
        self._since_retrain[key] = 0
        log.debug("IsolationForest retrained for %s on %d samples", key, len(buf))

    def push_and_predict(self, key: tuple, value: float) -> bool:
        """
        Add value to the buffer, retrain if due, predict anomaly.
        Returns True if the model flags this value as an anomaly.
        """
        buf = self._buffers[key]
        buf.append(value)
        self._since_retrain[key] += 1

        if len(buf) < self.WARMUP:
            return False  # still in warmup — not enough history yet

        # Train initial model or retrain periodically
        if key not in self._models or self._since_retrain[key] >= self.RETRAIN_EVERY:
            self._retrain(key)

        pred = self._models[key].predict([[value]])
        return int(pred[0]) == -1  # sklearn: -1 = outlier, 1 = inlier


# ── Main detector ──────────────────────────────────────────────────────────────
class AnomalyDetector:
    """
    Stateful anomaly detector combining four strategies.
    Call `analyze(event)` for each incoming metric event;
    returns a (possibly empty) list of AnomalyEvent objects.
    """

    def __init__(self):
        self._stats: dict[tuple, RollingStats] = defaultdict(
            lambda: RollingStats(maxlen=settings.anomaly_zscore_window)
        )
        self._prev:      dict[tuple, float]     = {}
        self._roc_stats: dict[tuple, RollingStats] = defaultdict(
            lambda: RollingStats(maxlen=20)
        )
        self._iforest = IsolationForestDetector()

    def analyze(self, event: dict) -> list[AnomalyEvent]:
        anomalies: list[AnomalyEvent] = []
        ts      = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        service = event["service"]
        node    = event.get("node", "unknown")

        for metric in MONITORED_METRICS:
            value = event.get(metric)
            if value is None:
                continue

            key   = (service, metric)
            stats = self._stats[key]

            # ── 1. Threshold detection ────────────────────────────────
            if metric in THRESHOLDS:
                warn_lvl, crit_lvl = THRESHOLDS[metric]
                if value >= crit_lvl:
                    anomalies.append(AnomalyEvent(
                        time=ts, service=service, node=node,
                        metric=metric, value=value,
                        threshold=crit_lvl, z_score=None,
                        severity="critical", method="threshold",
                    ))
                elif value >= warn_lvl:
                    anomalies.append(AnomalyEvent(
                        time=ts, service=service, node=node,
                        metric=metric, value=value,
                        threshold=warn_lvl, z_score=None,
                        severity="warning", method="threshold",
                    ))

            # ── 2. Z-score detection ──────────────────────────────────
            z = stats.z_score(value)
            if z is not None and abs(z) >= settings.anomaly_zscore_threshold:
                severity: Severity = "critical" if abs(z) >= 4.5 else "warning"
                anomalies.append(AnomalyEvent(
                    time=ts, service=service, node=node,
                    metric=metric, value=value,
                    threshold=None, z_score=round(z, 3),
                    severity=severity, method="zscore",
                ))

            # ── 3. Rate-of-change detection ───────────────────────────
            if key in self._prev:
                delta     = abs(value - self._prev[key])
                roc_stats = self._roc_stats[key]
                roc_z     = roc_stats.z_score(delta)
                roc_stats.push(delta)

                if roc_z is not None and roc_z >= 4.0:
                    anomalies.append(AnomalyEvent(
                        time=ts, service=service, node=node,
                        metric=metric, value=value,
                        threshold=None, z_score=round(roc_z, 3),
                        severity="warning", method="rate_of_change",
                    ))

            # ── 4. Isolation Forest (ML) ──────────────────────────────
            is_anomaly = self._iforest.push_and_predict(key, value)
            if is_anomaly:
                # Cross-check: only emit if z-score also looks elevated
                # (reduces false positives during the model's early warmup)
                cross_z = stats.z_score(value)
                if cross_z is None or abs(cross_z) >= 1.5:
                    ml_severity: Severity = "critical" if (
                        cross_z is not None and abs(cross_z) >= 3.0
                    ) else "warning"
                    anomalies.append(AnomalyEvent(
                        time=ts, service=service, node=node,
                        metric=metric, value=value,
                        threshold=None,
                        z_score=round(cross_z, 3) if cross_z is not None else None,
                        severity=ml_severity,
                        method="isolation_forest",
                    ))

            # Update rolling stats and previous value
            stats.push(value)
            self._prev[key] = value

        return anomalies
