# Changelog

All notable changes. Format: Keep a Changelog; semver-ish (major.minor.patch where
minor = new platform phase). Tag releases as vX.Y.Z on main.

## [0.7.0] — 2026-08-24 — Observability depth + release engineering

### Added
- Prometheus exporter for the Bronze consumer (`edp_consumer_*` counters,
  per-partition lag gauge) — env-gated via `METRICS_ENABLED/METRICS_PORT`.
- Seasonal anomaly baselines: median+MAD robust statistics over
  day-of-week/hour buckets with fallback ladder; `anomaly_use_seasonality`
  monitor flag eliminates daily/weekly cycle false-positives.
- OpenLineage v1 event emission (`run/start|complete|fail`) to Marquez-style
  endpoints, wired into Silver batch lifecycle; run_id shared across lineage,
  PipelineMonitor and Delta txn context. Never raises into the pipeline.
- CI workflow: lint / unit+Spark integration (Java 17) / terraform fmt+validate /
  Airflow DagBag integrity as separate jobs.
- CD workflow skeleton: WIF-authenticated, environment-gated terraform apply,
  plan-first DAG sync, tag→changelog verification.

## [0.6.0] — 2026-08-24 — Governance

- PII masking policies (tokenize/mask/partial/drop), pandas + Spark paths.
- GDPR Art.17 erasure propagation w/ Delta metrics capture + JSONL audit.
- Terraform baseline: least-privilege SAs, GCS lifecycle/UBLA buckets, BQ datasets.

## [0.5.0] — 2026-08-24 — Distributed data quality

- Spark-native expectation engine: N expectations → ONE aggregate pass.
- GE null semantics aligned across engines; pandas/Spark parity test.
- DQ promotion gate in silver DAG now full-scan (no driver sampling).

## [0.4.0] — 2026-08-24 — Orchestration

- Airflow DAGs (silver hourly w/ SLA+DQ gate, nightly maintenance, manual backfill).
- Monitored task callables; backfill runbook; local compose stack.

## [0.3.0] — 2026-08-24 — Silver layer + maintenance

- SCD Type 1/2 MERGE with xxhash64 change detection; quarantine-not-drop.
- OPTIMIZE/Z-ORDER/VACUUM runner with dry-run default.

## [0.2.0] — 2026-08-24 — Bronze streaming at scale

- Spark Structured Streaming Kafka→Delta path; wire-format decode;
  schema-ID allow-listing → quarantine; idempotent txn writes.

## [0.1.0] — 2026-08-24 — Production hardening

- confluent-kafka consumer rewrite (per-message lag bug eliminated),
  rebalance-safe flushes, headers-based DLQ, TLS/SASL default.
- Monitor: lazy BQ client, start-row persistence, stale-run reconciliation.
- DQ framework: durable GCS reports, severity ranks. Test suite established.
