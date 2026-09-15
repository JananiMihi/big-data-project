-- Serving-layer schema for the fleet pipeline.
-- Applied automatically on first Postgres container start (mounted into
-- /docker-entrypoint-initdb.d/), or manually via:
--   psql -h localhost -U fleet -d fleet -f storage/init_db.sql

-- Speed layer: latest windowed utilization metrics per zone.
CREATE TABLE IF NOT EXISTS realtime_utilization (
    window_start        TIMESTAMP NOT NULL,
    window_end           TIMESTAMP NOT NULL,
    zone                  TEXT NOT NULL,
    active_vehicles      INT NOT NULL,
    idle_vehicles        INT NOT NULL,
    enroute_vehicles     INT NOT NULL,
    on_trip_vehicles     INT NOT NULL,
    idle_ratio            DOUBLE PRECISION NOT NULL,
    trips_in_window      INT NOT NULL,
    earnings_in_window   DOUBLE PRECISION NOT NULL,
    computed_at           TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

-- Speed layer: latest known state per vehicle (drives idle/staleness alerts).
CREATE TABLE IF NOT EXISTS vehicle_status_current (
    vehicle_id     TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    zone            TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    trip_id         TEXT,
    fare            DOUBLE PRECISION,
    last_event_at   TIMESTAMP NOT NULL,
    status_since    TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- Observability: alerts raised by the health-check rules.
CREATE TABLE IF NOT EXISTS alerts (
    id           SERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,
    entity_id    TEXT,
    severity     TEXT NOT NULL,
    message      TEXT NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    resolved     BOOLEAN NOT NULL DEFAULT false
);

-- Batch layer: daily per-vehicle profitability reconciliation.
CREATE TABLE IF NOT EXISTS profitability_reconciliation (
    report_date        DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    trips_count         INT NOT NULL,
    distance_covered    DOUBLE PRECISION,
    gross_earnings      DOUBLE PRECISION NOT NULL,
    fuel_cost            DOUBLE PRECISION NOT NULL,
    maintenance_cost    DOUBLE PRECISION NOT NULL,
    net_profit           DOUBLE PRECISION NOT NULL,
    service_flag        BOOLEAN,
    is_unprofitable      BOOLEAN NOT NULL,
    computed_at          TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, vehicle_id)
);

-- Batch layer: idempotency ledger so the Airflow DAG never double-processes
-- a batch-drop file (matches the "orchestrated for historical analysis"
-- requirement without reprocessing side effects on retries).
CREATE TABLE IF NOT EXISTS processed_batch_files (
    file_name       TEXT PRIMARY KEY,
    report_date     DATE NOT NULL,
    row_count        INT,
    processed_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_realtime_utilization_zone ON realtime_utilization (zone, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON alerts (resolved, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reconciliation_date ON profitability_reconciliation (report_date);
