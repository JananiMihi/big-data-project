# Fleet Operations Data Pipeline (EC8203 Mini-Project)

End-to-end **Lambda architecture** data pipeline for **Use Case 1: Ride-Hailing
Fleet Operations** — live fleet utilization/earnings by zone, reconciled daily
against per-vehicle fuel/maintenance costs to flag vehicles that are becoming
unprofitable.

See [`REPORT.md`](REPORT.md) for the architecture decision (Lambda vs Kappa),
tech-stack justification, and observability design write-up required by the
assignment brief.

## Architecture

```
                         ┌─────────────────────┐
 sources/                │   Kafka              │
 streaming_source.py ───▶│ fleet.telemetry      │
 (GPS/status every 2s)   │  (3 partitions,      │
                         │   keyed by vehicle)  │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────▼────────────────┐
                    │  SPEED LAYER (Spark Structured  │
                    │  Streaming, processing/          │
                    │  speed_layer.py)                 │
                    │  - clean + zone-enrich            │
                    │  - 1-min windowed utilization      │
                    │  - latest per-vehicle state         │
                    └───────┬───────────────┬───────────┘
                            │               │
              ┌─────────────▼───┐   ┌───────▼────────────────┐
              │ Postgres         │   │ Parquet data lake        │
              │ realtime_        │   │ data/parquet_lake/trips/ │
              │ utilization,     │   │  (partitioned by date -   │
              │ vehicle_status_  │   │   master dataset, stands   │
              │ current          │   │   in for HDFS/S3)          │
              └────────┬─────────┘   └───────────┬────────────────┘
                       │                          │
 sources/               │           ┌──────────────▼───────────────┐
 batch_source.py ───────┼──────────▶│  BATCH LAYER (Airflow DAG,     │
 (1 CSV/simulated day)  │  CSV file │  airflow_dags/daily_           │
                        │           │  reconciliation_dag.py)         │
                        │           │  joins day's trips + expenses   │
                        │           └──────────────┬───────────────────┘
                        │                          │
                        │            ┌──────────────▼──────────────┐
                        │            │ Postgres                      │
                        │            │ profitability_reconciliation  │
                        │            └──────────────┬─────────────────┘
                        │                            │
              ┌─────────▼────────────────────────────▼───┐
              │  SERVING LAYER (FastAPI, serving/api.py)    │
              │  /metrics/realtime  /report/daily  /alerts   │
              └───────────────────────────────────────────────┘

 processing/health_check.py polls Postgres every 30s and raises alerts
 (stale telemetry, vehicle idle too long) — the observability layer.
```

## What's included

| Layer | Code |
|---|---|
| Streaming source (simulated) | [`sources/streaming_source.py`](sources/streaming_source.py) |
| Daily-batch source (simulated) | [`sources/batch_source.py`](sources/batch_source.py) |
| Speed layer (Spark Structured Streaming) | [`processing/speed_layer.py`](processing/speed_layer.py) |
| Batch layer (Airflow DAG) | [`airflow_dags/daily_reconciliation_dag.py`](airflow_dags/daily_reconciliation_dag.py) |
| Observability / alerting | [`processing/health_check.py`](processing/health_check.py) |
| Serving API | [`serving/api.py`](serving/api.py) |
| Postgres schema | [`storage/init_db.sql`](storage/init_db.sql) |

## Setup

On Windows, use `py -m pip` instead of plain `pip` because the `pip` command
is not always added to PATH. On macOS/Linux, `python -m pip` is the usual form.

```bash
# Windows
py -m pip install -r requirements.txt

# macOS/Linux
python -m pip install -r requirements.txt
```

You need a JVM available on PATH for Spark (`java -version`). Docker is
strongly recommended for Kafka, Postgres and Airflow:

```bash
docker compose up -d
```

The Postgres schema in `storage/init_db.sql` is applied automatically on
first container start. Then create the Kafka topic:

```bash
# Windows
py setup_topics.py

# macOS/Linux
python setup_topics.py
```

Airflow's web UI comes up at http://localhost:8080 (admin/admin) once the
`airflow` container finishes its first migration (~1-2 minutes). The DAG
`daily_fleet_reconciliation` is unpaused-by-default=false in Airflow, so
**unpause it in the UI** (or `docker exec fleet-airflow airflow dags unpause daily_fleet_reconciliation`)
before it will start picking up batch files.

## Running it

Open 4 terminals from the project folder.

**Terminal 1 — streaming source:**
```bash
# Windows
py -m sources.streaming_source --rate 1

# macOS/Linux
python -m sources.streaming_source --rate 1
```

**Terminal 2 — daily batch source** (default: 1 simulated day = 5 real minutes):
```bash
# Windows
py -m sources.batch_source

# macOS/Linux
python -m sources.batch_source
```

**Terminal 3 — speed layer (Spark):**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 processing/speed_layer.py
```

**Terminal 4 — observability + API:**
```bash
# Windows (run in separate terminals, or use a shell that supports background jobs)
py -m processing.health_check
uvicorn serving.api:app --port 8000

# macOS/Linux
python -m processing.health_check &
uvicorn serving.api:app --port 8000
```

The Airflow scheduler (running inside the `airflow` container) picks up
each new file `sources.batch_source` drops into `data/batch_drops/` and
reconciles it against that day's trips archived by the speed layer.

### Try the API

```bash
curl "http://localhost:8000/metrics/realtime"
curl "http://localhost:8000/vehicles?status=idle"
curl "http://localhost:8000/alerts"
curl "http://localhost:8000/report/daily?date=2026-09-14"
```

## Simulated clock

One simulated "day" = `SIMULATED_DAY_SECONDS` real seconds (default **300s /
5 minutes**, overridable via env var or `--day-seconds`). The batch source
labels each drop with a calendar date starting today and incrementing by one
day per drop, so the reconciliation report for a given date is keyed the
same way a real daily feed would be.

## Configuration

All tunables live in [`common/config.py`](common/config.py) and are
overridable via environment variables (Kafka bootstrap servers, Postgres
connection, fleet size, window duration, alert thresholds, etc.) — see that
file for the full list and defaults.

## Observability

- Every component logs structured JSON (one object per line, tagged with a
  `stage` field) via [`common/logging_utils.py`](common/logging_utils.py).
- `processing/health_check.py` polls the serving store every
  `HEALTH_CHECK_INTERVAL_SECONDS` (default 30s) and raises two rules into
  the `alerts` table: no telemetry received in `STALE_DATA_ALERT_MINUTES`,
  and a vehicle idle for longer than `IDLE_ALERT_MINUTES`. Alerts are
  deduplicated for `ALERT_DEDUPE_MINUTES` so a standing condition doesn't
  spam new rows every poll.
- The batch DAG also raises a `batch_data_quality` alert when a vehicle is
  billed expenses for a day with zero recorded trips.

## Limitations / assumptions

See "Limitations, trade-offs" in [`REPORT.md`](REPORT.md). In short: the
local filesystem stands in for HDFS/S3 (swap `PARQUET_LAKE_DIR` for an S3
path + s3a:// connector for production), Spark runs in local mode via
`spark-submit` rather than a managed cluster, and vehicle-state counts
within a window are "seen at least once in that state" rather than
time-weighted.
