"""Observability: basic health-check / alerting rules.

Polls the serving store (not the stream) so it stays decoupled from Spark,
and implements the two rules the assignment asks for at minimum:
  - no data received in N minutes (pipeline-level staleness)
  - a per-vehicle threshold rule (idle for too long)

Alerts are deduplicated so a standing condition doesn't spam a new row
every poll interval.

Run:
    python -m processing.health_check
"""
import logging
import time

from common.config import (
    ALERT_DEDUPE_MINUTES,
    HEALTH_CHECK_INTERVAL_SECONDS,
    IDLE_ALERT_MINUTES,
    STALE_DATA_ALERT_MINUTES,
)
from common.db import cursor
from common.logging_utils import get_logger, log

logger = get_logger("observability.health_check")


def raise_alert(alert_type: str, entity_id, severity: str, message: str):
    with cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM alerts
            WHERE alert_type = %s
              AND entity_id IS NOT DISTINCT FROM %s
              AND resolved = false
              AND created_at > now() - (%s || ' minutes')::interval
            """,
            (alert_type, entity_id, ALERT_DEDUPE_MINUTES),
        )
        if cur.fetchone():
            return  # already alerted recently, avoid spamming
        cur.execute(
            "INSERT INTO alerts (alert_type, entity_id, severity, message) VALUES (%s, %s, %s, %s)",
            (alert_type, entity_id, severity, message),
        )
    log(logger, logging.WARNING, message, alert_type=alert_type, entity_id=entity_id, severity=severity)


def check_pipeline_staleness():
    with cursor() as cur:
        cur.execute("SELECT max(last_event_at) AS most_recent FROM vehicle_status_current")
        row = cur.fetchone()
    most_recent = row["most_recent"] if row else None
    with cursor() as cur:
        cur.execute(
            "SELECT (%s::timestamp IS NULL OR %s::timestamp < now() - (%s || ' minutes')::interval) AS stale",
            (most_recent, most_recent, STALE_DATA_ALERT_MINUTES),
        )
        stale = cur.fetchone()["stale"]
    if stale:
        raise_alert(
            "no_data_received",
            None,
            "critical",
            f"No telemetry received in the last {STALE_DATA_ALERT_MINUTES} minutes - check the streaming source and Kafka.",
        )


def check_idle_vehicles():
    with cursor() as cur:
        cur.execute(
            """
            SELECT vehicle_id, status_since FROM vehicle_status_current
            WHERE status = 'idle'
              AND status_since < now() - (%s || ' minutes')::interval
            """,
            (IDLE_ALERT_MINUTES,),
        )
        idle_rows = cur.fetchall()
    for row in idle_rows:
        raise_alert(
            "vehicle_idle_too_long",
            row["vehicle_id"],
            "warning",
            f"Vehicle {row['vehicle_id']} has been idle for over {IDLE_ALERT_MINUTES} minutes.",
        )


def run_once():
    check_pipeline_staleness()
    check_idle_vehicles()


def run_forever():
    log(logger, logging.INFO, "health check loop starting", interval_seconds=HEALTH_CHECK_INTERVAL_SECONDS)
    while True:
        try:
            run_once()
        except Exception as exc:
            log(logger, logging.ERROR, "health check iteration failed", error=str(exc))
        time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_forever()
