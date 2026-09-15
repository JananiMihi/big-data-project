"""Lambda architecture - speed layer.

Spark Structured Streaming job that:
  1. Reads raw telemetry events from Kafka (`fleet.telemetry`).
  2. Enriches each event with an operating `zone` derived from lat/lon
     (grid bucketing) and clips out-of-range GPS noise (cleaning).
  3. Archives every enriched event to the local Parquet "data lake",
     partitioned by date - this is the durable master dataset the batch
     layer (Airflow) reprocesses later, which is *why* this is a Lambda
     and not a Kappa design: the speed layer's windowed view and the
     batch layer's daily reconciliation are deliberately two separate
     recomputations over the same raw data, at different latencies.
  4. Upserts each vehicle's latest known state into `vehicle_status_current`
     (drives the idle/staleness observability checks in health_check.py).
  5. Computes 1-minute tumbling-window utilization/earnings metrics per
     zone and upserts them into `realtime_utilization` (the API's
     /metrics/realtime source).

Run (from the project root, with a JVM + Spark available):
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        processing/speed_layer.py
"""
import logging

import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from common.config import (
    CHECKPOINT_DIR,
    CITY_LAT_RANGE,
    CITY_LON_RANGE,
    GRID_SIZE,
    KAFKA_BOOTSTRAP_SERVERS,
    PARQUET_LAKE_DIR,
    SPEED_LAYER_TRIGGER_INTERVAL,
    TELEMETRY_TOPIC,
    WINDOW_DURATION,
)
from common.db import get_connection
from common.logging_utils import get_logger, log

logger = get_logger("processing.speed_layer")

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("fare", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def zone_for(lat, lon):
    if lat is None or lon is None:
        return None
    lat_span = CITY_LAT_RANGE[1] - CITY_LAT_RANGE[0]
    lon_span = CITY_LON_RANGE[1] - CITY_LON_RANGE[0]
    lat_bin = int((lat - CITY_LAT_RANGE[0]) / lat_span * GRID_SIZE)
    lon_bin = int((lon - CITY_LON_RANGE[0]) / lon_span * GRID_SIZE)
    lat_bin = min(max(lat_bin, 0), GRID_SIZE - 1)
    lon_bin = min(max(lon_bin, 0), GRID_SIZE - 1)
    return f"Z{lat_bin}{lon_bin}"


def build_stream_df(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TELEMETRY_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    zone_udf = F.udf(zone_for, StringType())
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        # cleaning: drop events missing the fields every downstream step needs
        .filter(F.col("vehicle_id").isNotNull() & F.col("status").isNotNull())
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("zone", zone_udf(F.col("lat"), F.col("lon")))
        .withColumn("event_date", F.to_date("event_time"))
    )
    return parsed


def upsert_vehicle_status(rows):
    if not rows:
        return
    sql = """
        INSERT INTO vehicle_status_current
            (vehicle_id, status, zone, lat, lon, trip_id, fare, last_event_at, status_since)
        VALUES %s
        ON CONFLICT (vehicle_id) DO UPDATE SET
            status = EXCLUDED.status,
            zone = EXCLUDED.zone,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            trip_id = EXCLUDED.trip_id,
            fare = EXCLUDED.fare,
            last_event_at = EXCLUDED.last_event_at,
            status_since = CASE
                WHEN vehicle_status_current.status = EXCLUDED.status
                THEN vehicle_status_current.status_since
                ELSE EXCLUDED.last_event_at
            END,
            updated_at = now()
    """
    values = [
        (
            r.vehicle_id,
            r.status,
            r.zone,
            r.lat,
            r.lon,
            None if r.status == "idle" else r.trip_id,
            r.fare,
            r.event_time,
            r.event_time,
        )
        for r in rows
    ]
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
        conn.commit()
    finally:
        conn.close()


def upsert_realtime_utilization(rows):
    if not rows:
        return
    sql = """
        INSERT INTO realtime_utilization
            (window_start, window_end, zone, active_vehicles, idle_vehicles,
             enroute_vehicles, on_trip_vehicles, idle_ratio, trips_in_window, earnings_in_window)
        VALUES %s
        ON CONFLICT (window_start, zone) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            active_vehicles = EXCLUDED.active_vehicles,
            idle_vehicles = EXCLUDED.idle_vehicles,
            enroute_vehicles = EXCLUDED.enroute_vehicles,
            on_trip_vehicles = EXCLUDED.on_trip_vehicles,
            idle_ratio = EXCLUDED.idle_ratio,
            trips_in_window = EXCLUDED.trips_in_window,
            earnings_in_window = EXCLUDED.earnings_in_window,
            computed_at = now()
    """
    values = [
        (
            r.window_start,
            r.window_end,
            r.zone,
            r.active_vehicles,
            r.idle_vehicles,
            r.enroute_vehicles,
            r.on_trip_vehicles,
            r.idle_ratio,
            r.trips_in_window,
            r.earnings_in_window,
        )
        for r in rows
    ]
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
        conn.commit()
    finally:
        conn.close()


def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    batch_df.persist()
    try:
        count = batch_df.count()
        log(logger, logging.INFO, "processing micro-batch", batch_id=batch_id, events=count)

        # 1. Archive to the Parquet data lake (master dataset for the batch layer)
        (
            batch_df.write.mode("append")
            .partitionBy("event_date")
            .parquet(str(PARQUET_LAKE_DIR / "trips"))
        )

        # 2. Latest state per vehicle -> vehicle_status_current
        w = Window.partitionBy("vehicle_id").orderBy(F.col("event_time").desc())
        latest = (
            batch_df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .select("vehicle_id", "status", "zone", "lat", "lon", "trip_id", "fare", "event_time")
        )
        upsert_vehicle_status(latest.collect())

        # 3. 1-minute tumbling window utilization metrics per zone -> realtime_utilization
        agg = (
            batch_df.groupBy(F.window("event_time", WINDOW_DURATION), "zone")
            .agg(
                F.countDistinct("vehicle_id").alias("active_vehicles"),
                F.countDistinct(F.when(F.col("status") == "idle", F.col("vehicle_id"))).alias("idle_vehicles"),
                F.countDistinct(F.when(F.col("status") == "enroute", F.col("vehicle_id"))).alias("enroute_vehicles"),
                F.countDistinct(F.when(F.col("status") == "on_trip", F.col("vehicle_id"))).alias("on_trip_vehicles"),
                F.countDistinct(F.when((F.col("status") == "idle") & F.col("trip_id").isNotNull(), F.col("trip_id"))).alias("trips_in_window"),
                F.sum(F.when((F.col("status") == "idle") & F.col("trip_id").isNotNull(), F.col("fare")).otherwise(0.0)).alias("earnings_in_window"),
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .withColumn(
                "idle_ratio",
                F.when(F.col("active_vehicles") > 0, F.col("idle_vehicles") / F.col("active_vehicles")).otherwise(0.0),
            )
            .select(
                "window_start", "window_end", "zone", "active_vehicles", "idle_vehicles",
                "enroute_vehicles", "on_trip_vehicles", "idle_ratio", "trips_in_window", "earnings_in_window",
            )
        )
        upsert_realtime_utilization(agg.collect())
        log(logger, logging.INFO, "micro-batch committed", batch_id=batch_id, events=count)
    finally:
        batch_df.unpersist()


def main():
    spark = SparkSession.builder.appName("fleet-speed-layer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stream_df = build_stream_df(spark)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    query = (
        stream_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime=SPEED_LAYER_TRIGGER_INTERVAL)
        .start()
    )
    log(logger, logging.INFO, "speed layer started", topic=TELEMETRY_TOPIC, trigger=SPEED_LAYER_TRIGGER_INTERVAL)
    query.awaitTermination()


if __name__ == "__main__":
    main()
