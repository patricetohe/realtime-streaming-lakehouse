#!/usr/bin/env python3
"""Simulate an e-commerce clickstream and publish events to Kafka/Redpanda.

Usage:
    python producer/produce_events.py --num-events 100 --delay 0.1

Reads the broker address from KAFKA_BOOTSTRAP_SERVERS (default:
"localhost:19092", matching the external listener in docker-compose.yml)
unless --bootstrap-servers is passed explicitly.

Each event is a JSON object with a fixed schema (see `generate_event`):
event_id, user_id, session_id, event_type, category, product_id, price,
and an ISO-8601 UTC timestamp. Full execution (actually publishing to a
broker) requires a running Redpanda/Kafka instance, e.g. via
`docker compose up -d`; the event-generation logic itself has no such
dependency and is covered by tests/test_produce_events.py.
"""
import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from pydantic import ValidationError

from producer.schema import validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("produce_events")

DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092"
DEFAULT_TOPIC = "clickstream-events"

EVENT_TYPES = ["page_view", "search", "add_to_cart", "remove_from_cart", "purchase"]
CATEGORIES = ["electronics", "home", "books", "clothing", "toys", "sports", "grocery"]

_faker = Faker()


def generate_event(event_types=None, categories=None, faker=None):
    """Build one synthetic clickstream event as a plain dict.

    Schema:
        event_id    str (uuid4)
        user_id     str (uuid4, stable per simulated session caller may reuse)
        session_id  str (uuid4)
        event_type  str, one of EVENT_TYPES
        category    str, one of CATEGORIES
        product_id  str (SKU-like token)
        price       float, 2 decimal places
        timestamp   str, ISO-8601 UTC (e.g. "2026-10-01T12:00:00+00:00")
    """
    faker = faker or _faker
    event_types = event_types or EVENT_TYPES
    categories = categories or CATEGORIES

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "category": random.choice(categories),
        "product_id": f"SKU-{faker.random_number(digits=6, fix_len=True)}",
        "price": round(random.uniform(2.0, 500.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def event_to_json(event):
    """Serialize an event dict to a UTF-8 JSON string."""
    return json.dumps(event)


class ClickstreamProducer:
    """Thin wrapper around KafkaProducer for publishing clickstream events."""

    def __init__(self, bootstrap_servers=None, topic=None, client=None, validate=True):
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
        )
        self.topic = topic or DEFAULT_TOPIC
        self.validate = validate
        # Allow a pre-built client to be injected (used by tests to avoid
        # needing a real broker).
        self._producer = client or KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: event_to_json(v).encode("utf-8"),
        )

    def send(self, event):
        """Validate (unless disabled) and publish a single event dict.

        Raises ``pydantic.ValidationError`` instead of publishing when the
        event doesn't match the ``ClickstreamEvent`` schema, so malformed
        events never reach the broker silently.
        """
        if self.validate:
            try:
                validate_event(event)
            except ValidationError:
                logger.error("Dropping invalid event (failed schema validation): %s", event)
                raise
        self._producer.send(self.topic, value=event)

    def run(self, num_events, delay_seconds=0.0):
        """Generate and publish `num_events` events, sleeping between each."""
        for i in range(num_events):
            event = generate_event()
            self.send(event)
            logger.info("Sent event %d/%d: %s", i + 1, num_events, event["event_type"])
            if delay_seconds:
                time.sleep(delay_seconds)
        self._producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-events", type=int, default=100, help="Number of events to produce")
    parser.add_argument("--delay", type=float, default=0.1, help="Seconds to sleep between events")
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = ClickstreamProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
    producer.run(num_events=args.num_events, delay_seconds=args.delay)


if __name__ == "__main__":
    main()
