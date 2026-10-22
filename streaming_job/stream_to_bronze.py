"""Spark Structured Streaming job: read clickstream events from Kafka, parse JSON.

Usage:
    spark-submit streaming_job/stream_to_bronze.py

Reads raw JSON-encoded clickstream events from the `clickstream-events`
Kafka topic (see producer/produce_events.py and producer/schema.py for the
event shape) and parses them into a structured streaming DataFrame that
mirrors `producer.schema.ClickstreamEvent`.

This job also exposes `aggregate_events_per_category`, which applies a
watermark on event time and groups parsed events into 5-minute tumbling
windows per `category`, counting events per (window, category). This
bounds how much streaming state Spark has to keep around for late
events: once the watermark passes a window's end, that window's state
can be dropped. `run_windowed_aggregation` wires this into a second
streaming query (console sink; the silver/gold writes are separate,
later ROADMAP increments).

`write_to_bronze` persists parsed events to the Delta Lake bronze table
(append-only, raw/untransformed) and `run_bronze` wires the full
read-Kafka -> parse-JSON -> write-Delta-bronze pipeline together. This
is the durable landing zone the silver layer will later dedupe/clean
from.

Full execution requires a running Spark runtime (with the `delta-spark`
package available so `format("delta")` is registered) and a reachable
Kafka/Redpanda broker (`docker compose up -d`); the schema/parsing,
aggregation, and bronze-write logic itself has no streaming-broker
dependency and is covered by tests/test_stream_to_bronze.py, which
exercises `parse_events`, `aggregate_events_per_category`, and
`write_to_bronze` against a local-mode SparkSession and a batch
(non-streaming) DataFrame standing in for a micro-batch of Kafka
records.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DEFAULT_BOOTSTRAP_SERVERS = "redpanda:9092"
DEFAULT_TOPIC = "clickstream-events"
DEFAULT_WINDOW_DURATION = "5 minutes"
DEFAULT_WATERMARK_DELAY = "10 minutes"
DEFAULT_BRONZE_PATH = "data/lakehouse/bronze/clickstream_events"
DEFAULT_BRONZE_CHECKPOINT_PATH = "data/lakehouse/_checkpoints/bronze_clickstream_events"

# Mirrors producer.schema.ClickstreamEvent. Kept independent of pydantic
# here (the Spark job runs in a different process/environment than the
# producer); tests/test_stream_to_bronze.py guards against the two
# definitions drifting apart.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
    ]
)


def build_spark_session(app_name="realtime-streaming-lakehouse-bronze"):
    """Create (or fetch) the SparkSession used by this job."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_kafka_stream(spark, bootstrap_servers=None, topic=None, starting_offsets="latest"):
    """Open a streaming DataFrame reading raw records from the Kafka topic.

    Each row has the standard Kafka source columns (`key`, `value`,
    `topic`, `partition`, `offset`, `timestamp`, ...); `value` holds the
    raw JSON payload as bytes and is what `parse_events` consumes.
    """
    bootstrap_servers = bootstrap_servers or os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS
    )
    topic = topic or DEFAULT_TOPIC
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def parse_events(raw_df, schema=EVENT_SCHEMA):
    """Parse the JSON `value` column of a raw Kafka DataFrame into typed columns.

    Works on both streaming and batch DataFrames, as long as they carry a
    `value` column with the JSON payload (string or binary) -- this is
    what makes the function unit-testable without a real streaming query
    or broker (see tests/test_stream_to_bronze.py).

    Returns a DataFrame with one column per field in `schema`, plus
    `timestamp` cast from its raw ISO-8601 string (as emitted by
    `producer.produce_events.generate_event`) to an actual TimestampType.
    Rows whose `value` doesn't match `schema` get a null `event` struct
    from `from_json` and are dropped so malformed records never reach
    downstream layers.
    """
    parsed = raw_df.select(from_json(col("value").cast(StringType()), schema).alias("event")).select(
        "event.*"
    )
    parsed = parsed.dropna(subset=["event_id"])
    return parsed.withColumn("timestamp", to_timestamp(col("timestamp")))


def aggregate_events_per_category(
    parsed_df,
    window_duration=DEFAULT_WINDOW_DURATION,
    watermark_delay=DEFAULT_WATERMARK_DELAY,
):
    """Aggregate parsed events into tumbling windows of events per category.

    Applies a watermark of `watermark_delay` on the `timestamp` event-time
    column (produced by `parse_events`) so Spark knows how long to keep
    accumulating state for a window before treating it as final and
    dropping it -- bounding memory use for a long-running streaming query
    with out-of-order/late-arriving events. Events arriving more than
    `watermark_delay` after the max timestamp seen so far are dropped from
    the aggregation.

    Groups the watermarked events into non-overlapping `window_duration`
    tumbling windows (keyed on `timestamp`) together with `category`, and
    counts events per (window, category) bucket.

    Works against both streaming and batch DataFrames: `withWatermark` is
    accepted on a batch DataFrame too (it's a no-op there, since there's
    no incremental state to bound), which is what makes this testable
    without a running streaming query -- see
    tests/test_stream_to_bronze.py.

    Returns a DataFrame with columns `window_start`, `window_end`,
    `category`, and `event_count`.
    """
    return (
        parsed_df.withWatermark("timestamp", watermark_delay)
        .groupBy(window(col("timestamp"), window_duration), col("category"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("category"),
            col("count").alias("event_count"),
        )
    )


def run(bootstrap_servers=None, topic=None, output_mode="append", trigger_interval=None):
    """Build and start the read-Kafka -> parse-JSON streaming query.

    Writes to the console sink for now (no Delta Lake bronze write yet --
    see ROADMAP.md). Returns the running `StreamingQuery` so callers can
    `.awaitTermination()` or stop it (useful in tests/manual runs).
    """
    spark = build_spark_session()
    raw = read_kafka_stream(spark, bootstrap_servers=bootstrap_servers, topic=topic)
    parsed = parse_events(raw)

    writer = parsed.writeStream.format("console").outputMode(output_mode)
    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def run_windowed_aggregation(
    bootstrap_servers=None,
    topic=None,
    output_mode="update",
    trigger_interval=None,
    window_duration=DEFAULT_WINDOW_DURATION,
    watermark_delay=DEFAULT_WATERMARK_DELAY,
):
    """Build and start the read-Kafka -> parse-JSON -> windowed-aggregate query.

    Same source as `run`, but writes the watermarked, 5-minute tumbling
    window event-count-per-category aggregation (see
    `aggregate_events_per_category`) to the console sink instead of the
    raw parsed events. Default `output_mode="update"` since the
    aggregation revises counts for a window as more events for it arrive
    before the watermark closes it out.

    Returns the running `StreamingQuery` so callers can
    `.awaitTermination()` or stop it (useful in tests/manual runs).
    """
    spark = build_spark_session()
    raw = read_kafka_stream(spark, bootstrap_servers=bootstrap_servers, topic=topic)
    parsed = parse_events(raw)
    aggregated = aggregate_events_per_category(
        parsed, window_duration=window_duration, watermark_delay=watermark_delay
    )

    writer = aggregated.writeStream.format("console").outputMode(output_mode)
    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def write_to_bronze(
    parsed_df,
    bronze_path=None,
    checkpoint_path=None,
    output_mode="append",
    trigger_interval=None,
):
    """Persist parsed clickstream events to the Delta Lake bronze table.

    The bronze layer is the raw, append-only landing zone: events land
    here as-is (already parsed/typed by `parse_events`, but otherwise
    untransformed -- no dedup, no casting beyond what parsing already
    does). Silver-layer dedup/cleanup is a separate, later ROADMAP
    increment.

    Dispatches on `parsed_df.isStreaming` so the same function works for
    both the real streaming job and unit tests:

    - Streaming DataFrame: starts a `writeStream` query writing to
      `bronze_path` in Delta format, using `checkpoint_path` to track
      progress (required for exactly-once/resumable streaming writes).
      Returns the running `StreamingQuery`.
    - Batch DataFrame (e.g. a micro-batch stand-in used in tests): does
      a plain `write.format("delta")` to `bronze_path` and returns
      `None`, since there is no query to hand back.

    `bronze_path`/`checkpoint_path` default to `DEFAULT_BRONZE_PATH` /
    `DEFAULT_BRONZE_CHECKPOINT_PATH`, overridable via the `BRONZE_PATH` /
    `BRONZE_CHECKPOINT_PATH` environment variables or direct arguments
    (tests pass a temp directory explicitly).
    """
    bronze_path = bronze_path or os.environ.get("BRONZE_PATH", DEFAULT_BRONZE_PATH)

    if parsed_df.isStreaming:
        checkpoint_path = checkpoint_path or os.environ.get(
            "BRONZE_CHECKPOINT_PATH", DEFAULT_BRONZE_CHECKPOINT_PATH
        )
        writer = (
            parsed_df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path)
            .outputMode(output_mode)
        )
        if trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)
        return writer.start(bronze_path)

    parsed_df.write.format("delta").mode(output_mode).save(bronze_path)
    return None


def run_bronze(
    bootstrap_servers=None,
    topic=None,
    bronze_path=None,
    checkpoint_path=None,
    trigger_interval=None,
):
    """Build and start the read-Kafka -> parse-JSON -> write-Delta-bronze query.

    This is the bronze-layer counterpart to `run` (console sink) and
    `run_windowed_aggregation` (windowed console sink): same Kafka
    source and JSON parsing, but the sink is the Delta Lake bronze table
    instead of the console. Returns the running `StreamingQuery` so
    callers can `.awaitTermination()` or stop it.
    """
    spark = build_spark_session()
    raw = read_kafka_stream(spark, bootstrap_servers=bootstrap_servers, topic=topic)
    parsed = parse_events(raw)
    return write_to_bronze(
        parsed,
        bronze_path=bronze_path,
        checkpoint_path=checkpoint_path,
        trigger_interval=trigger_interval,
    )


def main():
    query = run()
    query.awaitTermination()


if __name__ == "__main__":
    main()
