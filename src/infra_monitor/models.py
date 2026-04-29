from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, BigInteger, Float,
    Boolean, DateTime, func,
)
from infra_monitor.database import Base


class Metric(Base):
    __tablename__ = "metrics"

    time           = Column(DateTime(timezone=True), primary_key=True, default=func.now())
    service        = Column(String(128), primary_key=True)
    node           = Column(String(64), nullable=False)
    cpu_percent    = Column(Float, nullable=False)
    memory_percent = Column(Float, nullable=False)
    latency_ms     = Column(Float, nullable=False)
    request_count  = Column(Integer, nullable=False)
    error_rate     = Column(Float, nullable=False)


class AggregatedMetric(Base):
    __tablename__ = "aggregated_metrics"

    time            = Column(DateTime(timezone=True), primary_key=True, default=func.now())
    service         = Column(String(128), primary_key=True)
    window_seconds  = Column(Integer, primary_key=True, default=60)
    avg_cpu         = Column(Float)
    max_cpu         = Column(Float)
    avg_memory      = Column(Float)
    max_memory      = Column(Float)
    avg_latency_ms  = Column(Float)
    p95_latency_ms  = Column(Float)
    max_latency_ms  = Column(Float)
    total_requests  = Column(BigInteger)
    avg_error_rate  = Column(Float)
    sample_count    = Column(Integer)


class Anomaly(Base):
    __tablename__ = "anomalies"

    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    time        = Column(DateTime(timezone=True), nullable=False, default=func.now())
    service     = Column(String(128), nullable=False)
    node        = Column(String(64))
    metric      = Column(String(64), nullable=False)
    value       = Column(Float, nullable=False)
    threshold   = Column(Float)
    z_score     = Column(Float)
    severity    = Column(String(16), nullable=False, default="warning")
    method      = Column(String(32), nullable=False, default="threshold")
    resolved    = Column(Boolean, nullable=False, default=False)
    resolved_at = Column(DateTime(timezone=True))
