# Data Engineering Mini-Project Report

**Module:** EC8203 — Applied Big Data Engineering
**Use case:** 1 — Ride-Hailing Fleet Operations
**Architecture:** Lambda

> This is a working draft covering every section the brief asks for. Sections
> marked **[fill in]** need screenshots/numbers captured from an actual run
> before submission — everything else is ready to defend in the viva.

---

## 1. Use case & business requirements

A ride-hailing operator needs two things that a single processing path
struggles to serve well at once:

1. **Live operational visibility** — dispatchers need to know, right now,
   how many vehicles are active/idle/en-route/on-trip in each zone, and what
   the fleet is earning, so they can rebalance supply.
2. **A trustworthy daily reconciliation** — finance needs to know, per
   vehicle, whether yesterday's trip earnings actually covered yesterday's
   fuel and maintenance spend, so they can flag vehicles that are quietly
   losing money.

Business question this pipeline answers: *"What is fleet utilization and
earnings by zone right now, and which vehicles are becoming unprofitable
once yesterday's fuel/maintenance costs are factored in?"*

Two data sources, as specified:

- **Streaming**: `sources/streaming_source.py` simulates 20 vehicles emitting
  a GPS/status event every ~2 seconds (`trip_id, driver_id, vehicle_id, lat,
  lon, speed, status, fare, timestamp`), following an idle → enroute →
  on_trip → idle state machine with a realistic fare that accrues during a
  trip.
- **Daily batch**: `sources/batch_source.py` drops one CSV per simulated day
  (`vehicle_id, fuel_cost, maintenance_cost, distance_covered,
  service_flag, date`), simulating an end-of-day extract from garages/fuel
  partners.

## 2. Architecture decision: Lambda vs Kappa

**Chosen: Lambda.**

The deciding factor is that this use case has **two genuinely different
recomputations over the same raw events, at different latencies and against
different data availability**:

| | Speed layer | Batch layer |
|---|---|---|
| Question answered | "What's happening right now?" | "What actually happened yesterday, correctly costed?" |
| Latency requirement | Seconds (dispatcher-facing) | Hours is fine (finance-facing) |
| Input available at compute time | Only the stream so far | The full day's trips **plus** a file that didn't exist yet when those trips happened |
| Correction/replay needs | None — a stale window just gets overwritten by the next one | Must be re-runnable/idempotent if the expense file arrives late or malformed |

Kappa (a single stream-processing path, replaying history through the same
job when you need to reprocess) is the more elegant choice when there is
*one* canonical transformation and "batch" is just "replaying more stream".
That's not true here: the daily reconciliation isn't a replay of the
telemetry stream through the same logic — it's a **join against data that
did not exist during streaming** (the expense file lands *after* the day's
trips happened). Forcing that join into the streaming job would mean either
(a) holding a full day of state in the streaming job just to wait for a file
that arrives once, which wastes streaming resources and fights Structured
Streaming's watermark/state model, or (b) re-ingesting the whole day's raw
events back through Kafka to "replay" them for the join, which is exactly
the operational complexity Kappa is supposed to avoid.

Lambda's speed/batch split maps directly onto the two real audiences
(dispatch vs. finance) and the two real latency budgets, and it means a bug
in the batch reconciliation logic can be fixed and *rerun* against the
archived Parquet data without touching, replaying, or slowing down the live
Spark Structured Streaming job. The cost we accept for this is the classic
Lambda trade-off: **two codepaths that both touch "utilization/earnings" and
must be kept conceptually consistent** (see Limitations, §6). Given the
2-week scope and that the two layers here answer genuinely different
questions rather than the same one twice, that cost is worth it.

**Rejected alternative — pure Kappa:** would need a single Structured
Streaming job stateful over ~24h+ windows to hold trip data until the daily
expense file shows up, which is a poor fit for Spark's streaming state store
at this data volume/latency profile, and couples the real-time dispatcher
view's uptime to the batch reconciliation logic's correctness.

## 3. Technology stack & justification

| Layer | Choice | Why (tied to this use case) |
|---|---|---|
| Ingestion | **Apache Kafka**, single topic `fleet.telemetry`, 3 partitions keyed by `vehicle_id` | Decouples the streaming source from the speed layer; partitioning by vehicle keeps one vehicle's events in order (needed for "latest state per vehicle"), while 3 partitions let the speed layer parallelize across zones. |
| Stream processing (speed layer) | **Spark Structured Streaming**, `foreachBatch` micro-batches, 15s trigger | One engine does cleaning, zone enrichment, windowed aggregation *and* Parquet archival in the same job — avoids running a second framework just to archive raw events for the batch layer. `foreachBatch` lets a single micro-batch write to two sinks (Postgres + Parquet) transactionally per batch, which a pure streaming sink API doesn't give you as simply. |
| Orchestration (batch layer) | **Apache Airflow**, 1-minute poll DAG | The batch layer isn't "run once nightly" here (simulated days compress to minutes), so it needs to react to new files quickly rather than on a fixed nightly cron — Airflow's short-interval DAG plus an idempotency ledger (`processed_batch_files`) gives retry-safe, exactly-once-effect processing, which a bare cron script would need to reinvent. |
| Storage — master dataset | **Parquet on local filesystem** (`data/parquet_lake/trips/`, partitioned by date) | Stands in for S3/HDFS at mini-project scale — same partition-pruned columnar read pattern the batch layer relies on (`event_date=YYYY-MM-DD` partitions), same code would point at `s3a://.../trips/` in production by changing one config value. |
| Storage — serving store | **PostgreSQL** | Both the speed layer's windowed metrics and the batch layer's reconciliation report are small, structured, point-queryable outputs (per zone/window, per vehicle/day) — a relational store with upserts (`ON CONFLICT`) is a much better fit than a wide-column store like Cassandra, which earns its keep at write-throughput/scale this project doesn't have. |
| Serving | **FastAPI** | Thin, typed REST layer over Postgres for the dashboard-equivalent deliverable (`/metrics/realtime`, `/report/daily`, `/alerts`, `/vehicles`); trivial to extend with a frontend later without touching the pipeline. |

## 4. Processing design

- **Cleaning**: events missing `vehicle_id`/`status` are dropped before any
  further processing (`processing/speed_layer.py::build_stream_df`).
- **Enrichment**: raw `lat`/`lon` is bucketed into a 3×3 operating-zone grid
  (`zone_for`) — turns continuous GPS into the categorical dimension the
  business question ("earnings by zone") actually needs.
- **Aggregation/windowing**: 1-minute tumbling windows per zone compute
  active/idle/enroute/on-trip vehicle counts, idle ratio, completed-trip
  count and earnings.
- **Join**: the batch layer joins the day's completed-trip aggregates
  (from the Parquet archive) against the expense CSV on `vehicle_id`,
  producing `net_profit = gross_earnings - fuel_cost - maintenance_cost`.

## 5. Observability design

| What | How | Why |
|---|---|---|
| Structured logs | Every component (`sources/*`, `processing/*`, `serving/api.py`, the Airflow DAG) logs one JSON object per line via `common/logging_utils.py`, tagged with a `stage` field | Uniform shape across ingestion/processing/storage stages means logs can be filtered/aggregated the same way regardless of which component emitted them, and would ship as-is to a log store (ELK/CloudWatch) in production. |
| Pipeline staleness alert | `processing/health_check.py` checks `max(last_event_at)` in `vehicle_status_current` every 30s; alerts if no telemetry in `STALE_DATA_ALERT_MINUTES` | Directly satisfies "no data received in N minutes" — catches a dead producer, a stuck Spark job, or a broken Kafka connection before a dispatcher notices the dashboard just looks quiet. |
| Per-vehicle idle-too-long alert | Same poll loop; flags any vehicle whose `status_since` (tracked via a `CASE` upsert that only resets on an actual status change) exceeds `IDLE_ALERT_MINUTES` | This *is* the use case's suggested output ("threshold-based alerts when a vehicle is considerably idle") — turns the speed layer's state table directly into an operational alert. |
| Batch data-quality alert | The Airflow DAG flags any vehicle billed in the expense file with zero recorded trips that day | Catches a mismatch between the two source systems (e.g. a vehicle was serviced but never dispatched) that would otherwise silently produce a nonsensical "100% loss" row in the reconciliation report. |
| Alert dedupe | Before inserting, `health_check.py` checks for an unresolved alert of the same type+entity within `ALERT_DEDUPE_MINUTES` | A standing condition (e.g. a genuinely broken producer) would otherwise write a new alert row every 30 seconds. |

## 6. Results

**[fill in after a live run]** — paste/screenshot:
- `curl localhost:8000/metrics/realtime` output showing per-zone utilization
- `curl localhost:8000/alerts` showing a triggered idle-vehicle or
  staleness alert
- `curl "localhost:8000/report/daily?date=..."` showing the profitability
  reconciliation, including at least one flagged unprofitable vehicle
- Airflow UI screenshot of `daily_fleet_reconciliation` runs succeeding

## 7. Limitations, trade-offs, and production changes

- **Dual codepaths**: utilization/earnings logic exists once in the speed
  layer (windowed, approximate) and once in the batch layer (exact,
  per-day) — the classic Lambda maintenance cost. At larger scale this is
  where a lot of teams either accept the duplication (as here) or migrate
  to Kappa once the batch-only join can be reframed as a stream join
  against a compacted Kafka topic for the expense data instead of a flat file.
- **Window semantics are approximate**: "active/idle/enroute/on-trip
  vehicle count" per window counts a vehicle if it was seen in that state
  *at least once* in the window, not time-weighted — a vehicle that
  flips state twice in one window is counted in both. Acceptable at a
  15s-trigger/1-minute-window granularity for a dispatcher glance; would
  need proper state-duration tracking for billing-grade accuracy.
- **Local filesystem instead of HDFS/S3**: swap `PARQUET_LAKE_DIR` for an
  `s3a://` path (with the Hadoop AWS connector on the Spark classpath) and
  no other code changes; not done here to keep the Docker footprint
  reasonable for a 2-week/laptop-run mini-project.
- **Spark local mode**: `spark-submit` runs against `local[*]`, not a real
  cluster — at production data volumes this would move to YARN/Kubernetes
  with the Kafka topic partition count scaled to the desired parallelism.
- **Single-broker Kafka, no replication**: fine for a demo; production
  needs `replication.factor >= 3` and multiple brokers for durability.
- **No end-to-end schema registry**: events are plain JSON validated by a
  Spark `StructType`, not Avro + Schema Registry (unlike the team's earlier
  Chapter 3 Kafka assignment) — a deliberate simplification given the
  extra moving parts (Spark's `from_json` needs a schema either way); a
  production system ingesting from multiple producers would want Avro/Schema
  Registry to catch producer-side schema drift before it reaches Spark.

## 8. Individual contributions

**[fill in if submitted as a group]**
