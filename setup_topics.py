"""Create the Kafka topics the pipeline needs (idempotent)."""
import logging

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KAFKA_BOOTSTRAP_SERVERS, TELEMETRY_TOPIC, TELEMETRY_TOPIC_PARTITIONS
from common.logging_utils import get_logger, log

logger = get_logger("ingestion.setup_topics")


def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics
    topics = [
        NewTopic(TELEMETRY_TOPIC, num_partitions=TELEMETRY_TOPIC_PARTITIONS, replication_factor=1),
    ]
    to_create = [t for t in topics if t.topic not in existing]
    if not to_create:
        log(logger, logging.INFO, "topics already exist, nothing to do", topics=[t.topic for t in topics])
        return
    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            log(logger, logging.INFO, "topic created", topic=topic)
        except Exception as exc:
            log(logger, logging.ERROR, "topic creation failed", topic=topic, error=str(exc))


if __name__ == "__main__":
    main()
