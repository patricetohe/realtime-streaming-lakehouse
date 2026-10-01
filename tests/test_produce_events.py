"""Unit tests for producer/produce_events.py.

No Kafka broker is needed: KafkaProducer is injected as a mock client,
so these tests exercise event-schema generation and the producer
wrapper's send/run logic in isolation.
"""
import json
from unittest.mock import MagicMock

from producer.produce_events import (
    CATEGORIES,
    EVENT_TYPES,
    ClickstreamProducer,
    event_to_json,
    generate_event,
)

EXPECTED_FIELDS = {
    "event_id",
    "user_id",
    "session_id",
    "event_type",
    "category",
    "product_id",
    "price",
    "timestamp",
}


def test_generate_event_has_expected_schema():
    event = generate_event()

    assert set(event.keys()) == EXPECTED_FIELDS
    assert event["event_type"] in EVENT_TYPES
    assert event["category"] in CATEGORIES
    assert isinstance(event["price"], float)
    assert event["price"] > 0


def test_generate_event_ids_are_unique_across_calls():
    first = generate_event()
    second = generate_event()

    assert first["event_id"] != second["event_id"]
    assert first["user_id"] != second["user_id"]
    assert first["session_id"] != second["session_id"]


def test_event_to_json_round_trips():
    event = generate_event()
    payload = event_to_json(event)

    assert isinstance(payload, str)
    assert json.loads(payload) == event


def test_clickstream_producer_send_publishes_to_configured_topic():
    mock_client = MagicMock()
    producer = ClickstreamProducer(topic="test-topic", client=mock_client)
    event = generate_event()

    producer.send(event)

    mock_client.send.assert_called_once_with("test-topic", value=event)


def test_clickstream_producer_run_sends_and_flushes():
    mock_client = MagicMock()
    producer = ClickstreamProducer(topic="test-topic", client=mock_client)

    producer.run(num_events=3, delay_seconds=0)

    assert mock_client.send.call_count == 3
    mock_client.flush.assert_called_once()
