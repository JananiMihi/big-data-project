"""Simulated streaming source: per-vehicle GPS/telemetry events.

Each of NUM_VEHICLES vehicles runs a small state machine
(idle -> enroute -> on_trip -> idle) and emits one JSON event every
EVENT_INTERVAL_SECONDS to the `fleet.telemetry` Kafka topic, keyed by
vehicle_id so all events for one vehicle land on the same partition
(ordering matters for the speed layer's "current state per vehicle" view).

Run:
    python -m sources.streaming_source
"""
import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

from common.config import (
    CITY_LAT_RANGE,
    CITY_LON_RANGE,
    EVENT_INTERVAL_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    NUM_VEHICLES,
    TELEMETRY_TOPIC,
)
from common.logging_utils import get_logger, log

logger = get_logger("ingestion.streaming_source")

# Trip fare model (LKR-ish demo units): flat pickup fee + per-tick increment
BASE_FARE = 150.0
FARE_INCREMENT_RANGE = (15.0, 45.0)
STEP_DEGREES = 0.004  # rough per-tick GPS jitter, keeps vehicles inside the city box


class Vehicle:
    def __init__(self, vehicle_id: str):
        self.vehicle_id = vehicle_id
        self.driver_id = f"D{vehicle_id[1:]}"
        self.status = "idle"
        self.lat = random.uniform(*CITY_LAT_RANGE)
        self.lon = random.uniform(*CITY_LON_RANGE)
        self.trip_id = None
        self.fare = 0.0

    def _move(self):
        self.lat = min(max(self.lat + random.uniform(-STEP_DEGREES, STEP_DEGREES), CITY_LAT_RANGE[0]), CITY_LAT_RANGE[1])
        self.lon = min(max(self.lon + random.uniform(-STEP_DEGREES, STEP_DEGREES), CITY_LON_RANGE[0]), CITY_LON_RANGE[1])

    def tick(self):
        self._move()
        roll = random.random()
        if self.status == "idle":
            if roll < 0.12:
                self.status = "enroute"
                self.trip_id = str(uuid.uuid4())
                self.fare = 0.0
        elif self.status == "enroute":
            if roll < 0.35:
                self.status = "on_trip"
                self.fare = BASE_FARE
        elif self.status == "on_trip":
            self.fare += random.uniform(*FARE_INCREMENT_RANGE)
            if roll < 0.18:
                self.status = "idle"
                # keep trip_id/fare on this final event so the speed layer
                # can attribute the completed trip's earnings, then clear.
                event = self._event()
                self.trip_id = None
                self.fare = 0.0
                return event
        return self._event()

    def _event(self):
        speed = 0.0 if self.status == "idle" else round(random.uniform(5, 45), 1)
        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": speed,
            "status": self.status,
            "fare": round(self.fare, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def delivery_report(err, msg):
    if err is not None:
        log(logger, logging.ERROR, "delivery failed", error=str(err))


def run(rate_hz: float, duration_seconds: Optional[float]):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    vehicles = [Vehicle(f"V{i:03d}") for i in range(1, NUM_VEHICLES + 1)]
    log(logger, logging.INFO, "starting streaming source", num_vehicles=NUM_VEHICLES, topic=TELEMETRY_TOPIC)

    sent = 0
    start = time.time()
    interval = 1.0 / rate_hz if rate_hz else EVENT_INTERVAL_SECONDS
    try:
        while duration_seconds is None or (time.time() - start) < duration_seconds:
            for vehicle in vehicles:
                event = vehicle.tick()
                producer.produce(
                    TELEMETRY_TOPIC,
                    key=vehicle.vehicle_id,
                    value=json.dumps(event),
                    callback=delivery_report,
                )
                sent += 1
            producer.poll(0)
            if sent % (NUM_VEHICLES * 10) == 0:
                log(logger, logging.INFO, "sent events", total_sent=sent)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        log(logger, logging.INFO, "streaming source stopped", total_sent=sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated fleet telemetry producer")
    parser.add_argument("--rate", type=float, default=1.0, help="ticks per second across the whole fleet (each tick emits one event per vehicle)")
    parser.add_argument("--duration", type=float, default=None, help="stop after N seconds (default: run forever)")
    args = parser.parse_args()
    run(args.rate, args.duration)
