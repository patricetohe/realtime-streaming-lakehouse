# Realtime Streaming Lakehouse

End-to-end real-time data pipeline demonstrating a modern streaming lakehouse architecture:

**Kafka (Redpanda) → Spark Structured Streaming → Delta Lake (bronze/silver/gold) → dbt**

## Architecture

```
[Event Producer] --> [Redpanda/Kafka topic] --> [Spark Structured Streaming]
                                                        |
                                                        v
                                        Bronze (raw) --> Silver (cleaned) --> Gold (aggregated, dbt)
                                                        |
                                                        v
                                                [Dashboard / BI]
```

- **producer/** — simulates a real-time event stream (e-commerce clickstream)
- **streaming_job/** — Spark Structured Streaming job consuming from Kafka, writing to Delta Lake
- **dbt_project/** — transforms the silver layer into gold, business-ready marts
- **tests/** — unit tests for producer and transformation logic
- **docs/** — architecture decision records and runbooks

## Status

This project is built incrementally, one small increment at a time — see [ROADMAP.md](ROADMAP.md) for the current backlog and progress.

## Stack

Python, Apache Kafka (Redpanda), Spark Structured Streaming, Delta Lake, dbt, Docker Compose, Great Expectations.

## Getting started

```bash
docker compose up -d          # start Redpanda + run the one-shot topic bootstrap job
python producer/produce_events.py
spark-submit streaming_job/stream_to_bronze.py
```

`docker compose up -d` starts the Redpanda broker and then runs `topic-init`
once to create the Kafka topics the pipeline needs (see
`scripts/bootstrap_topics.py`); it's idempotent, so re-running `up` is safe.
The broker exposes two listeners: containers on the compose network (like
`topic-init`, and later the Spark job) reach it at `redpanda:9092`, while
anything you run directly on your host — the producer included — reaches it
at `localhost:19092` (set `KAFKA_BOOTSTRAP_SERVERS=localhost:19092` if a
tool doesn't already default to it).

(Full setup instructions land as the corresponding roadmap items are completed.)
