"""Simulated daily-batch source: vehicle expense records from garages/fuel partners.

Once per simulated day (SIMULATED_DAY_SECONDS of real time), drops one CSV
file into data/batch_drops/ with one row per vehicle: fuel cost,
maintenance cost, distance covered and a service-due flag for that day.
The Airflow DAG (airflow_dags/daily_reconciliation_dag.py) polls this
directory and reconciles each new file against the speed layer's trip
archive.

Run:
    python -m sources.batch_source
"""
import argparse
import csv
import logging
import random
import time
from datetime import date, timedelta
from typing import Optional

from common.config import BATCH_DROP_DIR, NUM_VEHICLES, SIMULATED_DAY_SECONDS
from common.logging_utils import get_logger, log

logger = get_logger("ingestion.batch_source")

FIELDNAMES = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag", "date"]


def generate_day_file(sim_date: date):
    BATCH_DROP_DIR.mkdir(parents=True, exist_ok=True)
    path = BATCH_DROP_DIR / f"expenses_{sim_date.isoformat()}.csv"
    rows = []
    for i in range(1, NUM_VEHICLES + 1):
        vehicle_id = f"V{i:03d}"
        distance = round(random.uniform(20, 220), 1)
        rows.append(
            {
                "vehicle_id": vehicle_id,
                "fuel_cost": round(distance * random.uniform(8, 14), 2),
                "maintenance_cost": round(random.choice([0, 0, 0, random.uniform(500, 5000)]), 2),
                "distance_covered": distance,
                "service_flag": random.random() < 0.08,
                "date": sim_date.isoformat(),
            }
        )
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    log(logger, logging.INFO, "dropped daily batch file", path=str(path), rows=len(rows), sim_date=sim_date.isoformat())
    return path


def run(num_days: Optional[int], day_seconds: float):
    day_index = 0
    base_date = date.today()
    log(logger, logging.INFO, "starting batch source", day_seconds=day_seconds, num_days=num_days)
    try:
        while num_days is None or day_index < num_days:
            sim_date = base_date + timedelta(days=day_index)
            generate_day_file(sim_date)
            day_index += 1
            time.sleep(day_seconds)
    except KeyboardInterrupt:
        pass
    log(logger, logging.INFO, "batch source stopped", days_emitted=day_index)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated daily vehicle-expense batch source")
    parser.add_argument("--days", type=int, default=None, help="stop after emitting N daily files (default: run forever)")
    parser.add_argument("--day-seconds", type=float, default=SIMULATED_DAY_SECONDS, help="real seconds representing one simulated day")
    args = parser.parse_args()
    run(args.days, args.day_seconds)
