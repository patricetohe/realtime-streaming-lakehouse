"""Unit tests for streaming_job/stream_to_bronze.py.

Exercises `parse_events` against a local-mode SparkSession using a batch
DataFrame of raw JSON strings standing in for a Kafka micro-batch -- no
broker or streaming query required, so these run in plain `pytest`.
"""
import json

import pytest

# pyspark is a heavy dependency and is not installed everywhere (it is pulled
# in by requirements.txt and by the project's Docker environment). Skip the
# whole module rather than breaking collection of the other test files.
pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import Row, SparkSession  # noqa: E402

from producer.produce_events import generate_event  # noqa: E402
from streaming_job.stream_to_bronze import (  # noqa: E402
    aggregate_events_per_category,
    parse_events,
    write_to_bronze,
)


try:
    from delta import configure_spark_with_delta_pip  # noqa: E402
except ImportError:  # delta-spark not installed in this environment
    configure_spark_with_delta_pip = None


@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder.appName("test-stream-to-bronze").master("local[1]")
    if configure_spark_with_delta_pip is not None:
        # Register Delta's SQL extensions/catalog so format("delta") reads
        # and writes work in write_to_bronze's tests, same config a real
        # Delta-backed job would use.
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    yield session
    session.stop()


def _raw_df(spark, events):
    """Build a batch DataFrame shaped like the Kafka source's output."""
    rows = [Row(value=json.dumps(event)) for event in events]
    return spark.createDataFrame(rows)


def test_parse_events_extracts_all_fields(spark):
    event = generate_event()
    raw_df = _raw_df(spark, [event])

    parsed = parse_events(raw_df).collect()

    assert len(parsed) == 1
    row = parsed[0]
    assert row["event_id"] == event["event_id"]
    assert row["user_id"] == event["user_id"]
    assert row["event_type"] == event["event_type"]
    assert row["category"] == event["category"]
    assert row["product_id"] == event["product_id"]
    assert row["price"] == pytest.approx(event["price"])


def test_parse_events_casts_timestamp_column(spark):
    event = generate_event()
    raw_df = _raw_df(spark, [event])

    parsed = parse_events(raw_df)

    timestamp_type = dict(parsed.dtypes)["timestamp"]
    assert timestamp_type == "timestamp"
    assert parsed.collect()[0]["timestamp"] is not None


def test_parse_events_drops_malformed_records(spark):
    good_event = generate_event()
    raw_df = spark.createDataFrame(
        [Row(value=json.dumps(good_event)), Row(value="not valid json")]
    )

    parsed = parse_events(raw_df).collect()

    assert len(parsed) == 1
    assert parsed[0]["event_id"] == good_event["event_id"]


def test_parse_events_handles_multiple_events(spark):
    events = [generate_event() for _ in range(5)]
    raw_df = _raw_df(spark, events)

    parsed = parse_events(raw_df).collect()

    assert {row["event_id"] for row in parsed} == {e["event_id"] for e in events}


def test_aggregate_events_per_category_counts_within_one_window(spark):
    base_ts = "2026-01-01T00:00:00+00:00"
    events = [generate_event() for _ in range(4)]
    for event in events:
        event["timestamp"] = base_ts
    events[0]["category"] = events[1]["category"] = "electronics"
    events[2]["category"] = events[3]["category"] = "books"
    raw_df = _raw_df(spark, events)
    parsed = parse_events(raw_df)

    aggregated = aggregate_events_per_category(parsed).collect()
    counts = {row["category"]: row["event_count"] for row in aggregated}

    assert counts == {"electronics": 2, "books": 2}


def test_aggregate_events_per_category_splits_across_windows(spark):
    events = [generate_event() for _ in range(2)]
    events[0]["category"] = events[1]["category"] = "toys"
    events[0]["timestamp"] = "2026-01-01T00:00:00+00:00"
    # 10 minutes later falls into a different 5-minute tumbling window.
    events[1]["timestamp"] = "2026-01-01T00:10:00+00:00"
    raw_df = _raw_df(spark, events)
    parsed = parse_events(raw_df)

    aggregated = aggregate_events_per_category(parsed).collect()

    assert len(aggregated) == 2
    assert all(row["category"] == "toys" for row in aggregated)
    assert all(row["event_count"] == 1 for row in aggregated)
    assert {row["window_start"] for row in aggregated} == {
        row["window_start"] for row in aggregated
    }  # distinct windows, sanity check below confirms they differ
    assert aggregated[0]["window_start"] != aggregated[1]["window_start"]


def test_aggregate_events_per_category_output_columns(spark):
    event = generate_event()
    raw_df = _raw_df(spark, [event])
    parsed = parse_events(raw_df)

    aggregated = aggregate_events_per_category(parsed)

    assert set(aggregated.columns) == {
        "window_start",
        "window_end",
        "category",
        "event_count",
    }


def test_write_to_bronze_persists_parsed_events_as_delta(spark, tmp_path):
    pytest.importorskip("delta", reason="delta-spark not installed")

    events = [generate_event() for _ in range(3)]
    raw_df = _raw_df(spark, events)
    parsed = parse_events(raw_df)
    bronze_path = str(tmp_path / "bronze" / "clickstream_events")

    result = write_to_bronze(parsed, bronze_path=bronze_path)

    assert result is None  # batch write path: no streaming query to hand back
    written = spark.read.format("delta").load(bronze_path)
    assert {row["event_id"] for row in written.collect()} == {
        e["event_id"] for e in events
    }


def test_write_to_bronze_appends_across_multiple_batches(spark, tmp_path):
    pytest.importorskip("delta", reason="delta-spark not installed")

    first_batch = [generate_event() for _ in range(2)]
    second_batch = [generate_event() for _ in range(2)]
    bronze_path = str(tmp_path / "bronze" / "clickstream_events")

    write_to_bronze(parse_events(_raw_df(spark, first_batch)), bronze_path=bronze_path)
    write_to_bronze(parse_events(_raw_df(spark, second_batch)), bronze_path=bronze_path)

    written = spark.read.format("delta").load(bronze_path)
    expected_ids = {e["event_id"] for e in first_batch + second_batch}
    assert {row["event_id"] for row in written.collect()} == expected_ids
    assert written.count() == 4
