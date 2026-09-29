"""Serving layer: read-only API over the Postgres serving store.

This is the "consolidated report/dashboard" deliverable: the speed
layer's live utilization view, the batch layer's daily profitability
report, current per-vehicle state, and observability alerts, all in one
place.

Run:
    uvicorn serving.api:app --reload --port 8000
"""
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from common.db import cursor
from common.logging_utils import get_logger, log

logger = get_logger("serving.api")
app = FastAPI(title="Fleet Operations API", version="1.0")


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    dashboard_path = Path(__file__).resolve().parent / "dashboard.html"
    return dashboard_path.read_text(encoding="utf-8")


@app.middleware("http")
async def log_requests(request, call_next):
    response = await call_next(request)
    log(logger, logging.INFO, "request handled", path=request.url.path, method=request.method, status_code=response.status_code)
    return response


@app.get("/health")
def health():
    with cursor() as cur:
        cur.execute("SELECT max(last_event_at) AS most_recent FROM vehicle_status_current")
        row = cur.fetchone()
    return {"status": "ok", "most_recent_telemetry": row["most_recent"] if row else None}


@app.get("/metrics/realtime")
def realtime_metrics(zone: Optional[str] = Query(default=None, description="filter to one zone, e.g. Z11")):
    with cursor() as cur:
        if zone:
            cur.execute(
                """
                SELECT DISTINCT ON (zone) *
                FROM realtime_utilization
                WHERE zone = %s
                ORDER BY zone, window_start DESC
                """,
                (zone,),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT ON (zone) *
                FROM realtime_utilization
                ORDER BY zone, window_start DESC
                """
            )
        rows = cur.fetchall()
    return {"zones": rows}


@app.get("/vehicles")
def vehicles(status: Optional[str] = None):
    with cursor() as cur:
        if status:
            cur.execute("SELECT * FROM vehicle_status_current WHERE status = %s ORDER BY vehicle_id", (status,))
        else:
            cur.execute("SELECT * FROM vehicle_status_current ORDER BY vehicle_id")
        rows = cur.fetchall()
    return {"vehicles": rows}


@app.get("/alerts")
def alerts(resolved: bool = False, limit: int = 50):
    with cursor() as cur:
        cur.execute(
            "SELECT * FROM alerts WHERE resolved = %s ORDER BY created_at DESC LIMIT %s",
            (resolved, limit),
        )
        rows = cur.fetchall()
    return {"alerts": rows}


@app.get("/report/daily")
def daily_report(date: str = Query(..., description="YYYY-MM-DD, matches a batch-source drop date")):
    with cursor() as cur:
        cur.execute(
            "SELECT * FROM profitability_reconciliation WHERE report_date = %s ORDER BY net_profit ASC",
            (date,),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"no reconciliation report for {date} yet")
    unprofitable = [r for r in rows if r["is_unprofitable"]]
    return {
        "report_date": date,
        "vehicle_count": len(rows),
        "unprofitable_count": len(unprofitable),
        "total_net_profit": sum(r["net_profit"] for r in rows),
        "vehicles": rows,
    }
