"""
FastAPI Backend
───────────────
REST endpoints + WebSocket for the dashboard.

REST:
  GET /api/health                  — system health summary
  GET /api/services                — list of services with current health
  GET /api/metrics/recent          — last N raw metrics
  GET /api/metrics/{service}       — per-service time-series (1h default)
  GET /api/metrics/aggregated      — 1-min aggregates (all services)
  GET /api/anomalies               — recent anomalies (unresolved first)
  GET /api/anomalies/stats         — counts by severity / service
  PATCH /api/anomalies/{id}/resolve — mark anomaly resolved

WebSocket:
  WS /ws                           — real-time stream (anomalies + metric snapshots)
                                     Pushes every ~2s while Kafka consumer runs.
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.orm import Session

from infra_monitor.database import get_db, engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger(__name__)

# ── WebSocket connection manager ──────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._active.append(ws)
        log.info("WS client connected — total=%d", len(self._active))

    def disconnect(self, ws: WebSocket):
        self._active.remove(ws)
        log.info("WS client disconnected — total=%d", len(self._active))

    async def broadcast(self, payload: dict):
        dead = []
        for ws in self._active:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._active.remove(ws)


manager = ConnectionManager()

# ── Kafka anomaly consumer (background task) ──────────────────────────────────
async def kafka_broadcast_loop():
    """
    Consumes from the anomalies topic and broadcasts to WebSocket clients.
    Falls back gracefully if Kafka is unavailable.
    """
    bootstrap     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_anomaly = os.getenv("KAFKA_TOPIC_ANOMALIES", "infrastructure.anomalies")

    try:
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable

        await asyncio.sleep(10)  # wait for Kafka to be ready

        consumer = KafkaConsumer(
            topic_anomaly,
            bootstrap_servers=bootstrap,
            group_id="infra-api-ws",
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=200,
        )
        log.info("WS Kafka consumer started, topic=%s", topic_anomaly)

        while True:
            for msg in consumer:
                payload = {"type": "anomaly", "data": msg.value}
                await manager.broadcast(payload)
            await asyncio.sleep(0.1)

    except Exception as exc:
        log.warning("Kafka broadcast loop unavailable: %s", exc)


async def metrics_snapshot_loop():
    """Every 2 seconds, push the latest per-service snapshot to all WS clients."""
    await asyncio.sleep(5)
    while True:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT ON (service)
                        service, node, cpu_percent, memory_percent,
                        latency_ms, request_count, error_rate, time
                    FROM metrics
                    ORDER BY service, time DESC
                """)).mappings().all()

            snapshot = [dict(r) for r in rows]
            for s in snapshot:
                if isinstance(s.get("time"), datetime):
                    s["time"] = s["time"].isoformat()

            if snapshot:
                await manager.broadcast({"type": "metrics_snapshot", "data": snapshot})
        except Exception as exc:
            log.debug("Snapshot query failed: %s", exc)

        await asyncio.sleep(2)


# ── App lifecycle ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task1 = asyncio.create_task(kafka_broadcast_loop())
    task2 = asyncio.create_task(metrics_snapshot_loop())
    yield
    task1.cancel()
    task2.cancel()


app = FastAPI(
    title="Infrastructure Monitor API",
    version="1.0.0",
    description="Real-time infrastructure metrics, anomalies, and analytics.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the dashboard at /
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")
if os.path.isdir(DASHBOARD_DIR):
    app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root():
    dashboard_path = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.exists(dashboard_path):
        with open(dashboard_path, "r") as f:
            return f.read()
    return HTMLResponse("<h1>Infra Monitor API</h1><p><a href='/docs'>API Docs</a></p>")


@app.get("/api/health")
def health(db: Session = Depends(get_db)):
    """System-wide health summary."""
    try:
        row = db.execute(text("""
            SELECT
                COUNT(DISTINCT service)                         AS total_services,
                COUNT(DISTINCT node)                            AS total_nodes,
                ROUND(AVG(cpu_percent)::numeric, 2)             AS avg_cpu,
                ROUND(AVG(memory_percent)::numeric, 2)          AS avg_memory,
                ROUND(AVG(latency_ms)::numeric, 2)              AS avg_latency,
                ROUND(AVG(error_rate)::numeric, 2)              AS avg_error_rate,
                MAX(time)                                       AS last_event
            FROM (
                SELECT DISTINCT ON (service)
                    service, node, cpu_percent, memory_percent,
                    latency_ms, error_rate, time
                FROM metrics
                ORDER BY service, time DESC
            ) latest
        """)).mappings().one()

        anomaly_count = db.execute(text("""
            SELECT COUNT(*) FROM anomalies
            WHERE resolved = FALSE AND time > NOW() - INTERVAL '5 minutes'
        """)).scalar()

        r = dict(row)
        r["active_anomalies"] = anomaly_count
        r["status"] = (
            "critical" if anomaly_count >= 5 else
            "degraded" if anomaly_count >= 2 else
            "healthy"
        )
        if r.get("last_event") and isinstance(r["last_event"], datetime):
            r["last_event"] = r["last_event"].isoformat()
        return r
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}


@app.get("/api/services")
def get_services(db: Session = Depends(get_db)):
    """Latest snapshot per service with derived health status."""
    rows = db.execute(text("""
        WITH latest AS (
            SELECT DISTINCT ON (service)
                service, node, cpu_percent, memory_percent,
                latency_ms, request_count, error_rate, time
            FROM metrics
            ORDER BY service, time DESC
        ),
        recent_anomalies AS (
            SELECT service, COUNT(*) AS anomaly_count
            FROM anomalies
            WHERE resolved = FALSE AND time > NOW() - INTERVAL '5 minutes'
            GROUP BY service
        )
        SELECT
            l.*,
            COALESCE(ra.anomaly_count, 0) AS active_anomalies,
            CASE
                WHEN l.cpu_percent > 85 OR l.latency_ms > 2000 OR l.error_rate > 15
                    THEN 'critical'
                WHEN l.cpu_percent > 70 OR l.latency_ms > 1000 OR l.error_rate > 8
                    THEN 'degraded'
                ELSE 'healthy'
            END AS health
        FROM latest l
        LEFT JOIN recent_anomalies ra ON ra.service = l.service
        ORDER BY active_anomalies DESC, l.service
    """)).mappings().all()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("time"), datetime):
            d["time"] = d["time"].isoformat()
        result.append(d)
    return result


@app.get("/api/metrics/recent")
def recent_metrics(
    limit: int = Query(200, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT time, service, node, cpu_percent, memory_percent,
               latency_ms, request_count, error_rate
        FROM metrics
        ORDER BY time DESC
        LIMIT :limit
    """), {"limit": limit}).mappings().all()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("time"), datetime):
            d["time"] = d["time"].isoformat()
        result.append(d)
    return result


@app.get("/api/metrics/{service}")
def service_metrics(
    service: str,
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT time, cpu_percent, memory_percent, latency_ms,
               request_count, error_rate, node
        FROM metrics
        WHERE service = :service
          AND time > NOW() - (:minutes * INTERVAL '1 minute')
        ORDER BY time ASC
    """), {"service": service, "minutes": minutes}).mappings().all()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("time"), datetime):
            d["time"] = d["time"].isoformat()
        result.append(d)
    return {"service": service, "minutes": minutes, "data": result}


@app.get("/api/metrics/aggregated/summary")
def aggregated_metrics(
    minutes: int = Query(60, ge=5, le=1440),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT time, service, avg_cpu, max_cpu, avg_memory,
               avg_latency_ms, p95_latency_ms, total_requests, avg_error_rate
        FROM aggregated_metrics
        WHERE time > NOW() - (:minutes * INTERVAL '1 minute')
        ORDER BY service, time ASC
    """), {"minutes": minutes}).mappings().all()

    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("time"), datetime):
            d["time"] = d["time"].isoformat()
        result.append(d)
    return result


@app.get("/api/anomalies")
def get_anomalies(
    limit: int = Query(50, ge=1, le=500),
    resolved: bool = Query(False),
    service: str | None = Query(None),
    db: Session = Depends(get_db),
):
    filters = ["resolved = :resolved", "time > NOW() - INTERVAL '24 hours'"]
    params: dict[str, Any] = {"limit": limit, "resolved": resolved}

    if service:
        filters.append("service = :service")
        params["service"] = service

    where = " AND ".join(filters)

    rows = db.execute(text(f"""
        SELECT id, time, service, node, metric, value, threshold,
               z_score, severity, method, resolved, resolved_at
        FROM anomalies
        WHERE {where}
        ORDER BY time DESC
        LIMIT :limit
    """), params).mappings().all()

    result = []
    for r in rows:
        d = dict(r)
        for k in ("time", "resolved_at"):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        result.append(d)
    return result


@app.get("/api/anomalies/stats")
def anomaly_stats(db: Session = Depends(get_db)):
    by_severity = db.execute(text("""
        SELECT severity, COUNT(*) AS count
        FROM anomalies
        WHERE time > NOW() - INTERVAL '1 hour'
        GROUP BY severity
    """)).mappings().all()

    by_service = db.execute(text("""
        SELECT service, COUNT(*) AS count
        FROM anomalies
        WHERE resolved = FALSE AND time > NOW() - INTERVAL '1 hour'
        GROUP BY service
        ORDER BY count DESC
        LIMIT 10
    """)).mappings().all()

    by_metric = db.execute(text("""
        SELECT metric, COUNT(*) AS count
        FROM anomalies
        WHERE time > NOW() - INTERVAL '1 hour'
        GROUP BY metric
        ORDER BY count DESC
    """)).mappings().all()

    return {
        "by_severity": [dict(r) for r in by_severity],
        "top_failing_services": [dict(r) for r in by_service],
        "by_metric": [dict(r) for r in by_metric],
    }


@app.patch("/api/anomalies/{anomaly_id}/resolve")
def resolve_anomaly(anomaly_id: int, db: Session = Depends(get_db)):
    result = db.execute(text("""
        UPDATE anomalies
        SET resolved = TRUE, resolved_at = NOW()
        WHERE id = :id AND resolved = FALSE
        RETURNING id
    """), {"id": anomaly_id})
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Anomaly not found or already resolved")
    return {"id": anomaly_id, "resolved": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # Keep connection alive; data is pushed from background tasks
            await asyncio.sleep(30)
            await ws.send_json({"type": "ping"})
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
