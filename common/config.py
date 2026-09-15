"""Shared configuration for every component of the fleet pipeline.

Everything is overridable via environment variables so the same code runs
unchanged on a host machine and inside Docker/Airflow containers.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.environ.get("TELEMETRY_TOPIC", "fleet.telemetry")
TELEMETRY_TOPIC_PARTITIONS = int(os.environ.get("TELEMETRY_TOPIC_PARTITIONS", "3"))
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "fleet-speed-layer")

# --- Postgres (serving store) ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "fleet")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "fleet")

# --- Local "data lake" standing in for HDFS/S3 (see README for rationale) ---
PARQUET_LAKE_DIR = Path(os.environ.get("PARQUET_LAKE_DIR", str(PROJECT_ROOT / "data" / "parquet_lake")))
BATCH_DROP_DIR = Path(os.environ.get("BATCH_DROP_DIR", str(PROJECT_ROOT / "data" / "batch_drops")))
CHECKPOINT_DIR = Path(os.environ.get("CHECKPOINT_DIR", str(PROJECT_ROOT / "data" / "checkpoints" / "speed_layer")))

# --- Simulated clock ---
# One "simulated day" of batch-source activity compressed into this many
# real seconds. Default: 5 minutes/day, per the assignment's suggestion.
SIMULATED_DAY_SECONDS = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))

# --- Streaming source (telemetry simulator) ---
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "20"))
EVENT_INTERVAL_SECONDS = float(os.environ.get("EVENT_INTERVAL_SECONDS", "2.0"))

# City bounding box the simulated fleet operates in, divided into a
# GRID_SIZE x GRID_SIZE grid of operating zones (enrichment step).
CITY_LAT_RANGE = (6.85, 6.98)   # Colombo, Sri Lanka - arbitrary demo city
CITY_LON_RANGE = (79.83, 79.93)
GRID_SIZE = int(os.environ.get("GRID_SIZE", "3"))

# --- Speed layer processing ---
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
SPEED_LAYER_TRIGGER_INTERVAL = os.environ.get("SPEED_LAYER_TRIGGER_INTERVAL", "15 seconds")

# --- Observability thresholds ---
IDLE_ALERT_MINUTES = float(os.environ.get("IDLE_ALERT_MINUTES", "3"))
STALE_DATA_ALERT_MINUTES = float(os.environ.get("STALE_DATA_ALERT_MINUTES", "2"))
HEALTH_CHECK_INTERVAL_SECONDS = int(os.environ.get("HEALTH_CHECK_INTERVAL_SECONDS", "30"))
ALERT_DEDUPE_MINUTES = float(os.environ.get("ALERT_DEDUPE_MINUTES", "5"))
