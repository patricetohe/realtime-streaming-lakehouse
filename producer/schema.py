"""Pydantic schema + validation for clickstream events.

This formalizes the event contract that ``producer.produce_events``
generates informally today, so that:

- producer code can validate an event dict before publishing it, and
- the eventual Spark Structured Streaming job (and any consumer) has a
  single, importable source of truth for the expected fields/types.

No Kafka/Spark dependency here: this module is pure pydantic and is
covered by tests/test_schema.py.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

EventType = Literal["page_view", "search", "add_to_cart", "remove_from_cart", "purchase"]
Category = Literal["electronics", "home", "books", "clothing", "toys", "sports", "grocery"]


class ClickstreamEvent(BaseModel):
    """Validated shape of one e-commerce clickstream event.

    Mirrors the dict produced by ``producer.produce_events.generate_event``.
    """

    event_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    event_type: EventType
    category: Category
    product_id: str = Field(min_length=1)
    price: float = Field(gt=0)
    timestamp: datetime


def validate_event(event: dict) -> ClickstreamEvent:
    """Validate a raw event dict against the ``ClickstreamEvent`` schema.

    Returns the parsed model on success. Raises
    ``pydantic.ValidationError`` on malformed input (missing/extra
    field type mismatch, out-of-range price, unknown event_type or
    category, etc.) so callers can decide how to handle bad data
    (e.g. drop + log, route to a dead-letter topic).
    """
    return ClickstreamEvent.model_validate(event)


def is_valid_event(event: dict) -> bool:
    """Convenience boolean wrapper around ``validate_event``."""
    try:
        validate_event(event)
        return True
    except ValidationError:
        return False
