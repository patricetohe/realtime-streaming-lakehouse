"""Spark Structured Streaming job: read clickstream events from Kafka, parse JSON.

Usage:
    spark-submit streaming_job/stream_to_bronze.py

Reads raw JSON-encoded clickstream events from the `clickstream-events`
Kafka topic (see producer/produce_events.py and producer/schema.py for the
event shape) and parses them into a structured streaming DataFrame that
mirrors `producer.schema.ClickstreamEvent`.

This increment is intentionally scoped to "read from Kafka, parse JSON"
only (see ROADMAP.md) -- windowed aggregation and the Delta Lake bronze
write both land as separate, later increments. For now the parsed stream
is written to the console sink so the job is runnable end-to-end for
manual verification.

Full execution requires a running Spark runtime and a reachable
Kafka/Redpanda broker (`docker compose up -d`); the schema/parsing logic
itself has no such dependency and is covered by
tests/test_stream_to_bronze.py, which exercises `parse_events` against a
local-mode SparkSession and a batch (non-streaming) DataFrame standing in
for a micro-batch of Kafka records.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DEFAULT_BOOTSTRAP_SERVERS = "redpanda:9092"
DEFAULT_TOPIC = "clickstream-events"

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


def main():
    query = run()
    query.awaitTermination()


if __name__ == "__main__":
    main()
