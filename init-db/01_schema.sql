-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Services registry ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS services (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(128) UNIQUE NOT NULL,
    node        VARCHAR(64),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Raw metrics (time-series hypertable) ──────────────────────
CREATE TABLE IF NOT EXISTS metrics (
    time            TIMESTAMPTZ     NOT NULL,
    service         VARCHAR(128)    NOT NULL,
    node            VARCHAR(64)     NOT NULL,
    cpu_percent     DOUBLE PRECISION NOT NULL,
    memory_percent  DOUBLE PRECISION NOT NULL,
    latency_ms      DOUBLE PRECISION NOT NULL,
    request_count   INTEGER         NOT NULL,
    error_rate      DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_metrics_service_time ON metrics (service, time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_time ON metrics (time DESC);

-- ── 1-minute aggregated metrics ───────────────────────────────
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    time                TIMESTAMPTZ     NOT NULL,
    service             VARCHAR(128)    NOT NULL,
    window_seconds      INTEGER         NOT NULL DEFAULT 60,
    avg_cpu             DOUBLE PRECISION,
    max_cpu             DOUBLE PRECISION,
    avg_memory          DOUBLE PRECISION,
    max_memory          DOUBLE PRECISION,
    avg_latency_ms      DOUBLE PRECISION,
    p95_latency_ms      DOUBLE PRECISION,
    max_latency_ms      DOUBLE PRECISION,
    total_requests      BIGINT,
    avg_error_rate      DOUBLE PRECISION,
    sample_count        INTEGER
);

SELECT create_hypertable('aggregated_metrics', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_agg_service_time ON aggregated_metrics (service, time DESC);

-- ── Anomalies (regular table — no hypertable needed) ──────────
CREATE TABLE IF NOT EXISTS anomalies (
    id              BIGSERIAL       PRIMARY KEY,
    time            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    service         VARCHAR(128)    NOT NULL,
    node            VARCHAR(64),
    metric          VARCHAR(64)     NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    threshold       DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,
    severity        VARCHAR(16)     NOT NULL DEFAULT 'warning',
    method          VARCHAR(32)     NOT NULL DEFAULT 'threshold',
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_anomalies_service_time ON anomalies (service, time DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_resolved    ON anomalies (resolved, time DESC);

-- ── Continuous aggregate: per-minute service summary ─────────
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    service,
    AVG(cpu_percent)    AS avg_cpu,
    MAX(cpu_percent)    AS max_cpu,
    AVG(memory_percent) AS avg_memory,
    AVG(latency_ms)     AS avg_latency,
    MAX(latency_ms)     AS max_latency,
    SUM(request_count)  AS total_requests,
    AVG(error_rate)     AS avg_error_rate,
    COUNT(*)            AS samples
FROM metrics
GROUP BY bucket, service
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_1min',
    start_offset => INTERVAL '10 minutes',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);
