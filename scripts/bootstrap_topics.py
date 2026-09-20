#!/usr/bin/env python3
"""Bootstrap Kafka/Redpanda topics required by the streaming pipeline.

Usage:
    python scripts/bootstrap_topics.py

Reads the broker address from KAFKA_BOOTSTRAP_SERVERS (default:
"localhost:9092"). Creates each topic in TOPICS if it doesn't already
exist, retrying while the broker is still starting up. Idempotent: safe
to run every time the stack comes up (see the `topic-init` service in
docker-compose.yml).

Only needs `kafka-python` (see requirements.txt) to run. Full execution
requires a running Redpanda/Kafka broker, e.g. via `docker compose up -d`.
"""
import logging
import os
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

try:
    # Classic kafka-python raises this when no broker in the bootstrap
    # list could be reached.
    from kafka.errors import NoBrokersAvailable
except ImportError:  # pragma: no cover - depends on installed kafka client
    # Newer kafka-python releases (3.x) raise KafkaConnectionError instead.
    from kafka.errors import KafkaConnectionError as NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bootstrap_topics")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"

# Topics required by the pipeline: name -> (num_partitions, replication_factor)
TOPICS = {
    "clickstream-events": (3, 1),
}


def build_new_topics(topics=None):
    """Translate a {name: (partitions, replication)} config into NewTopic objects."""
    topics = topics or TOPICS
    return [
        NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
        for name, (partitions, replication) in topics.items()
    ]


def bootstrap(bootstrap_servers=None, topics=None, max_retries=10, retry_delay_seconds=3):
    """Create the pipeline's Kafka topics if they don't already exist.

    Idempotent (TopicAlreadyExistsError is treated as success) and retries
    for a while if the broker isn't reachable yet. Returns True on success,
    False if the broker could never be reached.
    """
    bootstrap_servers = bootstrap_servers or os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )
    new_topics = build_new_topics(topics)

    admin = None
    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="bootstrap-topics")
            break
        except NoBrokersAvailable:
            logger.warning(
                "Broker at %s not reachable yet (attempt %d/%d), retrying in %ds...",
                bootstrap_servers, attempt, max_retries, retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)
    else:
        logger.error("Could not reach Kafka broker at %s after %d attempts", bootstrap_servers, max_retries)
        return False

    created, skipped = [], []
    try:
        for topic in new_topics:
            try:
                admin.create_topics([topic])
                created.append(topic.name)
            except TopicAlreadyExistsError:
                skipped.append(topic.name)
    finally:
        admin.close()

    if created:
        logger.info("Created topics: %s", ", ".join(created))
    if skipped:
        logger.info("Already existed, skipped: %s", ", ".join(skipped))
    return True


if __name__ == "__main__":
    ok = bootstrap()
    sys.exit(0 if ok else 1)
