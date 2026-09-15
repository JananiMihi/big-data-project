"""Lambda architecture - batch layer.

Airflow DAG that reconciles each new daily vehicle-expense drop (the
batch source, sources/batch_source.py) against that day's raw trip
telemetry archived by the speed layer into the Parquet data lake. This
is the "historical analysis/reporting" half of the Lambda architecture:
it recomputes per-vehicle profitability from the same raw events the
speed layer already processed, joined against data (the daily expense
file) that was not available at ingest time.

Scheduled every minute so it reacts quickly to new batch-drop files
regardless of how compressed the simulated day is (SIMULATED_DAY_SECONDS);
each run is a cheap no-op if there is nothing new to process.
"""
import glob
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import BATCH_DROP_DIR, PARQUET_LAKE_DIR
from common.db import cursor
from common.logging_utils import get_logger, log

logger = get_logger("batch_layer.daily_reconciliation")

default_args = {
    "owner": "fleet-pipeline",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def find_new_batches(**context):
    with cursor() as cur:
        cur.execute("SELECT file_name FROM processed_batch_files")
        already_processed = {row["file_name"] for row in cur.fetchall()}

    all_files = sorted(glob.glob(str(BATCH_DROP_DIR / "expenses_*.csv")))
    new_files = [f for f in all_files if os.path.basename(f) not in already_processed]
    log(logger, logging.INFO, "batch scan complete", found=len(all_files), new=len(new_files))
    context["ti"].xcom_push(key="new_files", value=new_files)
    return new_files


def _load_trip_aggregates(report_date: str) -> pd.DataFrame:
    """Aggregate completed-trip events for report_date from the Parquet lake."""
    partition_dir = PARQUET_LAKE_DIR / "trips" / f"event_date={report_date}"
    if not partition_dir.exists():
        log(logger, logging.WARNING, "no trip archive for date, treating as zero trips", report_date=report_date)
        return pd.DataFrame(columns=["vehicle_id", "trips_count", "gross_earnings"])

    trips = pd.read_parquet(partition_dir)
    completed = trips[(trips["status"] == "idle") & trips["trip_id"].notna()]
    agg = (
        completed.groupby("vehicle_id")
        .agg(trips_count=("trip_id", "nunique"), gross_earnings=("fare", "sum"))
        .reset_index()
    )
    return agg


def process_batches(**context):
    new_files = context["ti"].xcom_pull(task_ids="find_new_batches", key="new_files") or []
    if not new_files:
        log(logger, logging.INFO, "no new batch files, skipping reconciliation")
        return

    for file_path in new_files:
        expenses = pd.read_csv(file_path)
        if expenses.empty:
            continue
        report_date = str(expenses["date"].iloc[0])

        trip_agg = _load_trip_aggregates(report_date)
        merged = expenses.merge(trip_agg, on="vehicle_id", how="left")
        merged["trips_count"] = merged["trips_count"].fillna(0).astype(int)
        merged["gross_earnings"] = merged["gross_earnings"].fillna(0.0)
        merged["net_profit"] = merged["gross_earnings"] - merged["fuel_cost"] - merged["maintenance_cost"]
        merged["is_unprofitable"] = merged["net_profit"] < 0

        rows = [
            (
                report_date,
                r.vehicle_id,
                int(r.trips_count),
                float(r.distance_covered),
                float(r.gross_earnings),
                float(r.fuel_cost),
                float(r.maintenance_cost),
                float(r.net_profit),
                bool(r.service_flag),
                bool(r.is_unprofitable),
            )
            for r in merged.itertuples()
        ]
        with cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO profitability_reconciliation
                        (report_date, vehicle_id, trips_count, distance_covered, gross_earnings,
                         fuel_cost, maintenance_cost, net_profit, service_flag, is_unprofitable)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
                        trips_count = EXCLUDED.trips_count,
                        distance_covered = EXCLUDED.distance_covered,
                        gross_earnings = EXCLUDED.gross_earnings,
                        fuel_cost = EXCLUDED.fuel_cost,
                        maintenance_cost = EXCLUDED.maintenance_cost,
                        net_profit = EXCLUDED.net_profit,
                        service_flag = EXCLUDED.service_flag,
                        is_unprofitable = EXCLUDED.is_unprofitable,
                        computed_at = now()
                    """,
                    row,
                )
            cur.execute(
                "INSERT INTO processed_batch_files (file_name, report_date, row_count) VALUES (%s, %s, %s) "
                "ON CONFLICT (file_name) DO NOTHING",
                (os.path.basename(file_path), report_date, len(rows)),
            )

        zero_trip_vehicles = merged[merged["trips_count"] == 0]["vehicle_id"].tolist()
        if zero_trip_vehicles:
            with cursor() as cur:
                cur.execute(
                    "INSERT INTO alerts (alert_type, entity_id, severity, message) VALUES (%s, %s, %s, %s)",
                    (
                        "batch_data_quality",
                        report_date,
                        "warning",
                        f"{len(zero_trip_vehicles)} vehicle(s) billed expenses on {report_date} with zero recorded trips: {zero_trip_vehicles}",
                    ),
                )
        log(
            logger,
            logging.INFO,
            "reconciliation committed",
            file=os.path.basename(file_path),
            report_date=report_date,
            vehicles=len(rows),
            unprofitable=int(merged["is_unprofitable"].sum()),
        )


with DAG(
    dag_id="daily_fleet_reconciliation",
    description="Batch layer: reconcile daily vehicle expenses against archived trip telemetry",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "batch-layer", "lambda-architecture"],
) as dag:
    t1 = PythonOperator(task_id="find_new_batches", python_callable=find_new_batches)
    t2 = PythonOperator(task_id="process_batches", python_callable=process_batches)
    t1 >> t2
