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
docker compose up -d          # start Redpanda + Spark
python producer/produce_events.py
spark-submit streaming_job/stream_to_bronze.py
```

(Full setup instructions land as the corresponding roadmap items are completed.)
