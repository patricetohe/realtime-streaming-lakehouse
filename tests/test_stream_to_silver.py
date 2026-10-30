"""Unit tests for streaming_job/stream_to_silver.py.

Exercises `cast_columns`, `dedupe_events`, `add_ingestion_ts`, and
`write_to_silver` against a local-mode SparkSession using batch
DataFrames standing in for bronze micro-batches -- no broker or
streaming query required, so these run in plain `pytest`.
"""
import pytest

# pyspark is a heavy dependency and is not installed everywhere (it is pulled
# in by requirements.txt and by the project's Docker environment). Skip the
# whole module rather than breaking collection of the other test files.
pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import Row, SparkSession  # noqa: E402

from producer.produce_events import generate_event  # noqa: E402
from streaming_job.stream_to_silver import (  # noqa: E402
    add_ingestion_ts,
    cast_columns,
    dedupe_events,
    transform_to_silver,
    write_to_silver,
)

try:
    from delta import configure_spark_with_delta_pip  # noqa: E402
except ImportError:  # delta-spark not installed in this environment
    configure_spark_with_delta_pip = None


@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder.appName("test-stream-to-silver").master("local[1]")
    if configure_spark_with_delta_pip is not None:
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


def _bronze_df(spark, events):
    """Build a batch DataFrame shaped like the bronze table's output.

    Unlike the raw Kafka source, bronze rows are already parsed into
    typed-ish columns (see stream_to_bronze.parse_events) -- but price
    and timestamp are passed through as plain strings here to exercise
    `cast_columns` re-casting them to their canonical silver dtypes.
    """
    rows = [
        Row(
            event_id=event["event_id"],
            user_id=event["user_id"],
            session_id=event["session_id"],
            event_type=event["event_type"],
            category=event["category"],
            product_id=event["product_id"],
            price=str(event["price"]),
            timestamp=event["timestamp"],
        )
        for event in events
    ]
    return spark.createDataFrame(rows)


def test_cast_columns_casts_price_and_timestamp(spark):
    event = generate_event()
    df = _bronze_df(spark, [event])
    assert dict(df.dtypes)["price"] == "string"

    casted = cast_columns(df)

    dtypes = dict(casted.dtypes)
    assert dtypes["price"] == "double"
    assert dtypes["timestamp"] == "timestamp"
    row = casted.collect()[0]
    assert row["price"] == pytest.approx(event["price"])
    assert row["timestamp"] is not None


def test_cast_columns_ignores_missing_columns(spark):
    event = generate_event()
    df = _bronze_df(spark, [event]).drop("product_id")

    casted = cast_columns(df)

    assert "product_id" not in casted.columns
    assert dict(casted.dtypes)["price"] == "double"


def test_dedupe_events_removes_duplicate_event_ids(spark):
    event = generate_event()
    df = _bronze_df(spark, [event, event, event])
    casted = cast_columns(df)

    deduped = dedupe_events(casted).collect()

    assert len(deduped) == 1
    assert deduped[0]["event_id"] == event["event_id"]


def test_dedupe_events_keeps_distinct_events(spark):
    events = [generate_event() for _ in range(3)]
    df = _bronze_df(spark, events)
    casted = cast_columns(df)

    deduped = dedupe_events(casted).collect()

    assert {row["event_id"] for row in deduped} == {e["event_id"] for e in events}


def test_add_ingestion_ts_adds_non_null_timestamp_column(spark):
    event = generate_event()
    df = cast_columns(_bronze_df(spark, [event]))

    stamped = add_ingestion_ts(df)

    assert dict(stamped.dtypes)["ingestion_ts"] == "timestamp"
    assert stamped.collect()[0]["ingestion_ts"] is not None


def test_transform_to_silver_dedupes_casts_and_stamps(spark):
    event = generate_event()
    df = _bronze_df(spark, [event, event])

    silver = transform_to_silver(df)
    rows = silver.collect()

    assert len(rows) == 1  # duplicate dropped
    row = rows[0]
    assert row["price"] == pytest.approx(event["price"])  # cast applied
    assert row["ingestion_ts"] is not None  # stamp applied
    assert set(silver.columns) == {
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "category",
        "product_id",
        "price",
        "timestamp",
        "ingestion_ts",
    }


def test_write_to_silver_persists_transformed_events_as_delta(spark, tmp_path):
    pytest.importorskip("delta", reason="delta-spark not installed")

    events = [generate_event() for _ in range(3)]
    bronze_df = _bronze_df(spark, events)
    silver_df = transform_to_silver(bronze_df)
    silver_path = str(tmp_path / "silver" / "clickstream_events")

    result = write_to_silver(silver_df, silver_path=silver_path)

    assert result is None  # batch write path: no streaming query to hand back
    written = spark.read.format("delta").load(silver_path)
    assert {row["event_id"] for row in written.collect()} == {
        e["event_id"] for e in events
    }
    assert "ingestion_ts" in written.columns
