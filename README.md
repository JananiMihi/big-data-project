# Fleet Operations Data Pipeline (EC8203 Mini-Project)

End-to-end **Lambda architecture** data pipeline for **Use Case 1: Ride-Hailing
Fleet Operations** — live fleet utilization/earnings by zone, reconciled daily
against per-vehicle fuel/maintenance costs to flag vehicles that are becoming
unprofitable.



## Architecture

![Figure 1: Fleet Operations Data Pipeline](media/figure%201.png)

Figure 1. Fleet Operations Data Pipeline architecture showing the streaming, batch, and serving layers with observability checks.

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

```powershell
# After downloading the Spark binary archive on Windows
tar -xzf "C:\spark-downloads\spark-3.5.1-bin-hadoop3.tgz" -C "C:\"
Rename-Item "C:\spark-3.5.1-bin-hadoop3" "spark"
$env:SPARK_HOME = "C:\spark"
$env:Path = "$env:SPARK_HOME\bin;$env:Path"
spark-submit --version
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
```powershell
# Windows PowerShell; repeat these settings in each new terminal
$env:SPARK_HOME = "C:\spark"
$env:Path = "$env:SPARK_HOME\bin;$env:Path"
$env:PYSPARK_PYTHON = (py -c "import sys; print(sys.executable)")
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:PYTHONPATH = "$PWD;$env:PYTHONPATH"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 processing/speed_layer.py
```

```bash
# macOS/Linux
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

