from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Kafka ──────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_metrics: str = Field("infrastructure.metrics", alias="KAFKA_TOPIC_METRICS")
    kafka_topic_anomalies: str = Field("infrastructure.anomalies", alias="KAFKA_TOPIC_ANOMALIES")
    kafka_consumer_group: str = Field("infra-processor", alias="KAFKA_CONSUMER_GROUP")

    # ── Database ───────────────────────────────────────────────
    database_url: str = Field(
        "postgresql+psycopg2://monitor:monitor_secret@localhost:5432/infra_monitor",
        alias="DATABASE_URL",
    )

    # ── Generator ──────────────────────────────────────────────
    emit_interval_seconds: float = Field(1.0, alias="EMIT_INTERVAL_SECONDS")

    # ── Anomaly detection thresholds ──────────────────────────
    anomaly_cpu_threshold: float = Field(85.0, alias="ANOMALY_CPU_THRESHOLD")
    anomaly_memory_threshold: float = Field(90.0, alias="ANOMALY_MEMORY_THRESHOLD")
    anomaly_latency_threshold: float = Field(2000.0, alias="ANOMALY_LATENCY_THRESHOLD")
    anomaly_error_rate_threshold: float = Field(15.0, alias="ANOMALY_ERROR_RATE_THRESHOLD")
    anomaly_zscore_window: int = Field(60, alias="ANOMALY_ZSCORE_WINDOW")
    anomaly_zscore_threshold: float = Field(3.0, alias="ANOMALY_ZSCORE_THRESHOLD")


settings = Settings()
