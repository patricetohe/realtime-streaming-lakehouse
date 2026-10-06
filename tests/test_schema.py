"""Unit tests for producer/schema.py (pydantic event validation)."""
import pytest
from pydantic import ValidationError

from producer.produce_events import generate_event
from producer.schema import ClickstreamEvent, is_valid_event, validate_event


def test_validate_event_accepts_a_generated_event():
    event = generate_event()

    validated = validate_event(event)

    assert isinstance(validated, ClickstreamEvent)
    assert validated.event_id == event["event_id"]
    assert validated.price == event["price"]


def test_is_valid_event_true_for_generated_event():
    assert is_valid_event(generate_event()) is True


def test_validate_event_rejects_unknown_event_type():
    event = generate_event()
    event["event_type"] = "not_a_real_event_type"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_validate_event_rejects_unknown_category():
    event = generate_event()
    event["category"] = "not_a_real_category"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_validate_event_rejects_non_positive_price():
    event = generate_event()
    event["price"] = -5.0

    with pytest.raises(ValidationError):
        validate_event(event)


def test_validate_event_rejects_missing_field():
    event = generate_event()
    del event["product_id"]

    with pytest.raises(ValidationError):
        validate_event(event)


def test_validate_event_rejects_malformed_timestamp():
    event = generate_event()
    event["timestamp"] = "not-a-timestamp"

    with pytest.raises(ValidationError):
        validate_event(event)


def test_is_valid_event_false_for_bad_event():
    event = generate_event()
    event["price"] = 0

    assert is_valid_event(event) is False
