"""Silver layer: dedupe, cast types, add ingestion_ts.

Usage:
    spark-submit streaming_job/stream_to_silver.py

Reads the bronze Delta table written by `streaming_job.stream_to_bronze`
(raw, append-only, untransformed clickstream events) and applies the
silver-layer cleanup described in ROADMAP.md:

- `cast_columns` re-casts every column to its canonical silver dtype
  (defensive: a Delta table read back can surface columns as the types
  they were written with, and this keeps silver's schema authoritative
  regardless of how bronze stored them).
- `dedupe_events` drops duplicate events (same `event_id`/`timestamp`
  pair), which matters because Kafka producers/consumers in this
  pipeline only guarantee at-least-once delivery -- a retried publish or
  a replayed micro-batch after a restart can land the same event in
  bronze twice. On a streaming DataFrame this applies a watermark on
  `timestamp` first so Spark can bound how long it keeps per-key dedup
  state instead of accumulating it for the lifetime of the query.
- `add_ingestion_ts` stamps each row with the wall-clock time it was
  processed into silver, distinct from the event's own `timestamp`, so
  downstream consumers can reason about processing lag.

`transform_to_silver` composes the three in order (cast -> dedupe ->
stamp) and `write_to_silver` persists the result to the Delta Lake
silver table, mirroring `stream_to_bronze.write_to_bronze`'s
streaming-vs-batch dispatch so the same function works for both the
real streaming job and unit tests. `run_silver` wires the full
read-bronze -> transform -> write-Delta-silver pipeline together.

Full execution requires a running Spark runtime (with `delta-spark`
available) and a bronze Delta table already populated by
`stream_to_bronze.run_bronze` (`docker compose up -d`, then run that job
first); the transform logic itself has no streaming-broker dependency
and is covered by tests/test_stream_to_silver.py, which exercises
`cast_columns`, `dedupe_events`, `add_ingestion_ts`, and
`write_to_silver` against a local-mode SparkSession and a batch
DataFrame standing in for a bronze micro-batch.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, StringType, TimestampType

from streaming_job.stream_to_bronze import DEFAULT_BRONZE_PATH, DEFAULT_WATERMARK_DELAY

DEFAULT_SILVER_PATH = "data/lakehouse/silver/clickstream_events"
DEFAULT_SILVER_CHECKPOINT_PATH = "data/lakehouse/_checkpoints/silver_clickstream_events"

# Canonical silver dtypes, keyed by column name. Mirrors the bronze/event
# schema in stream_to_bronze.EVENT_SCHEMA; kept separate (rather than
# imported and reused) since silver is free to diverge (e.g. widen a
# type) without forcing a bronze schema change.
SILVER_CAST_TYPES = {
    "event_id": StringType(),
    "user_id": StringType(),
    "session_id": StringType(),
    "event_type": StringType(),
    "category": StringType(),
    "product_id": StringType(),
    "price": DoubleType(),
    "timestamp": TimestampType(),
}


def build_spark_session(app_name="realtime-streaming-lakehouse-silver"):
    """Create (or fetch) the SparkSession used by this job."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def cast_columns(df, cast_types=None):
    """Cast each configured column to its target silver dtype.

    Only touches columns that are both present on `df` and listed in
    `cast_types`, so this is safe to run against a bronze DataFrame that
    may carry extra/fewer columns than the full silver set.
    """
    cast_types = cast_types if cast_types is not None else SILVER_CAST_TYPES
    for column, dtype in cast_types.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))
    return df


def dedupe_events(
    df,
    dedupe_columns=("event_id", "timestamp"),
    watermark_column="timestamp",
    watermark_delay=DEFAULT_WATERMARK_DELAY,
):
    """Drop duplicate events, keeping one row per `dedupe_columns` key.

    On a streaming DataFrame, a watermark is applied on
    `watermark_column` first -- per Spark's streaming-deduplication
    semantics, `dropDuplicates` on a watermarked column lets Spark drop
    dedup state for keys older than the watermark instead of keeping it
    for the lifetime of the query. `dedupe_columns` includes the
    watermark column by default to satisfy that requirement.

    On a batch DataFrame (e.g. a bronze micro-batch stand-in used in
    tests), `withWatermark` is skipped -- it is streaming-only -- and
    this is a plain `dropDuplicates`.
    """
    if df.isStreaming:
        df = df.withWatermark(watermark_column, watermark_delay)
    return df.dropDuplicates(list(dedupe_columns))


def add_ingestion_ts(df):
    """Stamp each row with the wall-clock time it was processed into silver.

    Distinct from the event's own `timestamp` (when the click happened):
    `ingestion_ts` records when this pipeline run processed the row,
    which is what lets downstream consumers reason about processing lag
    between an event occurring and it landing in silver.
    """
    return df.withColumn("ingestion_ts", current_timestamp())


def transform_to_silver(bronze_df, dedupe_columns=("event_id", "timestamp"), cast_types=None):
    """Apply the full bronze -> silver transformation: cast, dedupe, stamp.

    Order matters: columns are cast first so dedup/downstream comparisons
    act on consistent types, then duplicates are dropped, then
    `ingestion_ts` is stamped on what remains. Works on both streaming
    and batch bronze DataFrames.
    """
    casted = cast_columns(bronze_df, cast_types=cast_types)
    deduped = dedupe_events(casted, dedupe_columns=dedupe_columns)
    return add_ingestion_ts(deduped)


def read_bronze(spark, bronze_path=None):
    """Open a streaming DataFrame reading the bronze Delta table (silver source)."""
    bronze_path = bronze_path or os.environ.get("BRONZE_PATH", DEFAULT_BRONZE_PATH)
    return spark.readStream.format("delta").load(bronze_path)


def write_to_silver(
    silver_df,
    silver_path=None,
    checkpoint_path=None,
    output_mode="append",
    trigger_interval=None,
):
    """Persist the transformed silver DataFrame to the Delta Lake silver table.

    Dispatches on `silver_df.isStreaming`, mirroring
    `stream_to_bronze.write_to_bronze`, so the same function works for
    both the real streaming job and unit tests:

    - Streaming DataFrame: starts a `writeStream` query writing to
      `silver_path` in Delta format, using `checkpoint_path` to track
      progress. Returns the running `StreamingQuery`.
    - Batch DataFrame: does a plain `write.format("delta")` to
      `silver_path` and returns `None`.

    `silver_path`/`checkpoint_path` default to `DEFAULT_SILVER_PATH` /
    `DEFAULT_SILVER_CHECKPOINT_PATH`, overridable via the `SILVER_PATH` /
    `SILVER_CHECKPOINT_PATH` environment variables or direct arguments
    (tests pass a temp directory explicitly).
    """
    silver_path = silver_path or os.environ.get("SILVER_PATH", DEFAULT_SILVER_PATH)

    if silver_df.isStreaming:
        checkpoint_path = checkpoint_path or os.environ.get(
            "SILVER_CHECKPOINT_PATH", DEFAULT_SILVER_CHECKPOINT_PATH
        )
        writer = (
            silver_df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path)
            .outputMode(output_mode)
        )
        if trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)
        return writer.start(silver_path)

    silver_df.write.format("delta").mode(output_mode).save(silver_path)
    return None


def run_silver(bronze_path=None, silver_path=None, checkpoint_path=None, trigger_interval=None):
    """Build and start the read-bronze -> transform-silver -> write-Delta-silver query.

    This is the silver-layer counterpart to `stream_to_bronze.run_bronze`:
    same Delta-backed pipeline shape, but sourced from the bronze table
    instead of Kafka, and sinking to the silver table after
    dedupe/cast/ingestion_ts cleanup. Returns the running
    `StreamingQuery` so callers can `.awaitTermination()` or stop it.
    """
    spark = build_spark_session()
    bronze = read_bronze(spark, bronze_path=bronze_path)
    silver = transform_to_silver(bronze)
    return write_to_silver(
        silver,
        silver_path=silver_path,
        checkpoint_path=checkpoint_path,
        trigger_interval=trigger_interval,
    )


def main():
    query = run_silver()
    query.awaitTermination()


if __name__ == "__main__":
    main()
