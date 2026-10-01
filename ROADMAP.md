# Roadmap

Small, incremental steps — roughly one per day. Check items off as they land; each one is its own commit/PR.

- [x] Docker Compose: Redpanda broker + topic bootstrap script
- [x] Python producer simulating e-commerce clickstream events (JSON schema)
- [ ] Event schema definition + validation (pydantic)
- [ ] Spark Structured Streaming job: read from Kafka, parse JSON
- [ ] Watermarking + 5-minute tumbling window aggregation (events per category)
- [ ] Write raw events to Delta Lake bronze layer
- [ ] Silver layer: dedupe, cast types, add ingestion_ts
- [ ] dbt project scaffold pointed at the lakehouse tables
- [ ] Gold layer: dbt model aggregating sessions per user
- [ ] Great Expectations suite validating gold layer row counts / nulls
- [ ] Unit tests for producer event generation
- [ ] Unit tests for streaming transformation logic
- [ ] GitHub Actions CI: run unit tests on every PR
- [ ] Architecture decision record: why Delta Lake over Iceberg here
- [ ] Checkpointing + exactly-once semantics notes and config
- [ ] Streamlit dashboard reading the gold layer for live metrics
- [ ] Schema evolution handling test (new optional field)
- [ ] Load-testing script for the producer (throughput ramp)
- [ ] Runbook: what to do when the streaming job falls behind
- [ ] Terraform skeleton for a cloud deployment (MSK + EMR, optional)
