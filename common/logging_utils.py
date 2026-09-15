"""Structured (JSON) logging shared by every pipeline stage.

Each log line is a single JSON object with a `stage` field (ingestion,
speed_layer, batch_layer, serving, observability) so logs from every
component can be filtered/aggregated the same way once shipped to a log
store. This satisfies the assignment's observability requirement for
"structured logging across ingestion, processing, and storage stages".
"""
import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "stage": getattr(record, "stage", record.name),
            "message": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(stage: str) -> logging.Logger:
    """Return a logger that emits one JSON object per line, tagged with `stage`."""
    logger = logging.getLogger(stage)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    class _StageFilter(logging.Filter):
        def filter(self, record):
            record.stage = stage
            return True

    if not any(isinstance(f, _StageFilter) for f in logger.filters):
        logger.addFilter(_StageFilter())
    return logger


def log(logger: logging.Logger, level: int, message: str, **fields):
    """Convenience wrapper: log(logger, logging.INFO, "sent event", vehicle_id=v, zone=z)."""
    logger.log(level, message, extra={"fields": fields})
