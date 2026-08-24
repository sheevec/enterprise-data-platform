# Enterprise Data Platform

A production-grade lakehouse platform implementing the Medallion architecture on GCP: distributed streaming ingestion from Kafka, SCD-aware Silver merges on Delta Lake, single-pass distributed data quality, SLA-driven Airflow orchestration, and policy-as-code governance. Built and verified as a portfolio-scale reference implementation of an architecture that runs financial-services workloads at 500TB / 200+ pipelines / 99.9% SLA scale.

---

## Implementation Status

Every component below is implemented, typed (mypy strict-clean), linted, and covered by tests that run against **real Spark + Delta sessions locally** (no mocks for the hard parts):

| Component | Module | What's real |
|---|---|---|
| Kafka edge consumer | `src/ingestion/kafka_consumer.py` | confluent-kafka/librdkafka; cooperative-sticky; rebalance-safe flush+commit; headers-based DLQ; background lag sampling |
| Schema governance | `src/ingestion/schema_registry.py` | wire-format decode cache; reader-schema resolution; CI backward-compat check |
| Bronze streaming | `src/processing/bronze_streaming.py` | Spark Structured Streaming Kafka→Delta; vectorized Avro decode; schema-ID allow-listing → quarantine; idempotent `txnAppId/txnVersion` writes |
| Silver processing | `src/processing/bronze_to_silver.py` | dedup-latest windows; quarantine-not-drop validation; SCD Type 1 & Type 2 MERGE with xxhash64 change detection |
| Table maintenance | `src/maintenance/table_optimization.py` | OPTIMIZE/Z-ORDER with partition-filter cost bounds; VACUUM dry-run default, 7-day retention floor |
| Distributed DQ | `src/validation/distributed_dq.py` | N expectations → ONE aggregate pass over the FULL table; GE null semantics; parity-tested vs pandas engine; violation extraction |
| DQ framework | `src/validation/data_quality.py` | tiered thresholds/severities; HTML+JSON reports (GCS-durable); Slack/PagerDuty routing; BQ score history |
| Observability | `src/observability/pipeline_monitor.py` | run metrics lifecycle in BQ; z-score volume/duration anomalies; freshness SLA sweeps; stale-run reconciliation |
| Orchestration | `airflow/dags/` | hourly silver pipeline w/ DQ promotion gate; nightly maintenance in off-peak window; parametrized manual backfill; every task booked into PipelineMonitor |
| PII protection | `src/processing/pii_masking.py` | tokenize (HMAC, join-preserving) / mask_full / last4 / drop; pandas + native-Spark paths; identifier-injection guard |
| GDPR erasure | `src/governance/gdpr_erasure.py` | cross-layer Delta DELETE propagation; operation-metrics capture; immutable JSONL audit trail |
| Infrastructure | `terraform/` | least-privilege per-workload SAs (impersonation, no keys); GCS versioning/lifecycle/UBLA/public-prevention; BQ dataset containers |

**Test suite: 107 passing** (Spark-backed integration included), mypy/flake8/black/isort clean.

---

## Overview

The Enterprise Data Platform unifies batch and streaming ingestion into a Medallion lakehouse, enforces quality at every layer boundary, and exposes curated marts through BigQuery. Deployed GCP-primary with AWS DR/burst, fully Terraform-managed.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│  Core Banking │ Trading Systems │ Market Data │ CRM │ External APIs  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Kafka (Confluent SR, SASL_SSL)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw / Immutable)                   │
│  • Spark Structured Streaming → Delta on GCS                         │
│  • Confluent wire-format decode; unknown schema IDs quarantined      │
│  • Partitioned source/date/hour; idempotent txn writes               │
│  • Edge consumer (confluent-kafka) for low-volume topics             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  bronze_to_silver (per micro-batch)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleansed / Conformed)              │
│  • Validate → quarantine (never silent drop)                         │
│  • Dedup-latest per business key (row_number window)                 │
│  • SCD Type 1 / Type 2 MERGE, xxhash64 change detection              │
│  • PII policy applied at this boundary                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt transformations (Gold models: roadmap)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (Business / Aggregated)               │
│  • Domain marts in BigQuery; OPTIMIZE/Z-ORDER'd Delta upstream       │
│  • DQ gate blocks promotion on P1 failures                           │
└───────────────────────────┬─────────────────────────────────────────┘
                            ▼
              BigQuery Analytics / Self-Serve APIs
```

### Key architectural decisions

- **Delta Lake** — ACID on object storage; MERGE for CDC; time travel powers safe backfills and RESTORE-based rollback.
- **confluent-kafka + Spark Structured Streaming** — C-speed edge consumer where a daemon fits; JVM-native streaming where throughput does. Both share one schema-registry client and one Bronze record contract (`_kafka_topic/_partition/_offset`).
- **Idempotency everywhere** — Delta `txnAppId/txnVersion` writes plus keyed MERGEs mean any replay (crash recovery, backfill, DAG clear-and-rerun) is a no-op or converges.
- **Quarantine over drops** — invalid rows land in typed Delta paths with machine-readable reasons; silent data loss is a design bug here.
- **Single-pass validation** — the DQ engine compiles an entire suite into one `df.agg()` scan; full-table checks cost one job regardless of expectation count.

---

## Data Quality Framework

Validation tiers (Bronze 0.90 / Silver 0.95 / Gold 0.99 pass-rate thresholds), severity bands (P1 within 5pp of threshold breach → page; else alert/log), DQS scores persisted to BigQuery per run with GCS-durable HTML reports.

The **distributed engine** (`validate_spark_distributed`) validates full tables in-cluster:

- Every expectation compiles to violation-flag + denominator aggregates merged into a single Spark pass
- Great Expectations null semantics: nulls violate `not_null`, are excluded from `between/in_set/regex`
- Duplicate detection via count-vs-countDistinct (no shuffling window)
- Violation extraction returns offending rows with per-expectation reasons for incident payloads
- Parity test guarantees identical scoring to the pandas engine

The Airflow silver DAG enforces this as a **promotion gate**: P1 failures block downstream propagation and route to PagerDuty.

---

## Orchestration

| DAG | Schedule | Chain | Notes |
|---|---|---|---|
| `edp_silver_hourly` | hourly | freshness sweep → Silver MERGE → DQ gate | 45-min task SLA; `max_active_runs=1` prevents merge collisions |
| `edp_maintenance_nightly` | 02:00 UTC | stale-run reconciliation → OPTIMIZE/Z-ORDER → VACUUM | off-peak window; vacuum dry-run by default |
| `edp_backfill_manual` | manual | Bronze availableNow → Silver availableNow | parametrized via `dag_run.conf`; idempotent replays |

Task callables live Airflow-free in `src/orchestration/spark_jobs.py` and are wrapped by `monitored()` — every execution books start/end rows into PipelineMonitor, feeding the same anomaly detection as Spark jobs.

Backfill safety model, verification SQL, and rollback procedure: [`airflow/runbooks/BACKFILL_RUNBOOK.md`](airflow/runbooks/BACKFILL_RUNBOOK.md).

---

## Governance & Compliance

### PII protection (policy-as-code)

Rules are declarative `PiiRule(column, strategy)` lists reviewable like code:

- `tokenize` — HMAC-SHA256 keyed tokens, deterministic so joins survive masking; reversible only with the key (key-per-subject enables crypto-shredding). Key injected via Vault/Secret Manager; fail-fast when missing.
- `mask_partial_last4` / `mask_full` — display-safe formats, length preserved
- `drop` — column removal
- Spark path uses native expressions with an identifier guard against rule-file injection

### GDPR Article 17 erasure

`GdprEraser` propagates subject deletion across Bronze/Silver/Gold Delta tables via keyed DELETE predicates, captures actual `numDeletedRows` from Delta operation metrics, and writes an **immutable JSONL audit record** (who/what/how-many) that is never erased. Honest caveats encoded in docs: time-travel history purges only after VACUUM past retention; crypto-shredding pattern referenced where legal requires erasure from immutable media.

### IAM baseline (Terraform)

One service account per workload (bronze/silver/maintenance/airflow/dq-monitor); grants are bucket- and dataset-scoped, never project-wide admin; Airflow impersonates workload SAs (`serviceAccountTokenCreator`) instead of sharing keys. Buckets ship with UBLA, public-access-prevention, versioning, and Standard→Nearline→Coldline lifecycle tiering.

---

## Cost Design

Strategies baked into the implementation:

- **Lifecycle tiering** automated in Terraform (30/90d defaults; silver at 60/180d)
- **Compaction + Z-ORDER** nightly, bounded by partition filters so OPTIMIZE doesn't scan history
- **Off-peak scheduling** for maintenance (02:00 UTC window)
- **Spot-friendly Dataproc** submit configs; AQE skew-join splitting enabled cluster-wide
- **VACUUM dry-run default** — deletion is always an explicit reviewed act
- **Slot-vs-on-demand discipline**: monitoring tables sized for cheap streaming inserts; ad-hoc analytics isolated

---

## Project Structure

```
enterprise-data-platform/
├── src/
│   ├── ingestion/
│   │   ├── kafka_consumer.py        # confluent-kafka edge consumer (Bronze writer)
│   │   └── schema_registry.py       # shared Confluent SR client + compat check
│   ├── processing/
│   │   ├── bronze_streaming.py      # Spark Structured Streaming Kafka → Delta
│   │   ├── bronze_to_silver.py      # dedup/quarantine/SCD MERGE pipeline
│   │   └── pii_masking.py           # tokenize/mask policies (pandas + Spark)
│   ├── validation/
│   │   ├── data_quality.py          # framework: suites, scores, alerts, reports
│   │   ├── distributed_dq.py        # Spark-native single-pass engine
│   │   └── dq_runner.py             # spark-submit promotion gate entrypoint
│   ├── observability/
│   │   └── pipeline_monitor.py      # metrics, anomalies, SLAs, incidents
│   ├── orchestration/
│   │   └── spark_jobs.py            # airflow-free task callables + monitor wrap
│   ├── governance/
│   │   └── gdpr_erasure.py          # Art.17 propagation + audit trail
│   ├── maintenance/
│   │   └── table_optimization.py    # OPTIMIZE/Z-ORDER/VACUUM runner
│   └── utils/config.py              # env parsing helpers
├── airflow/
│   ├── dags/                        # edp_silver_hourly / maintenance / backfill
│   ├── docker-compose.yml           # local Airflow 2.8 stack
│   └── runbooks/BACKFILL_RUNBOOK.md
├── terraform/
│   ├── main.tf                      # root wiring (env via -var-file)
│   ├── environments/{staging,prod}/
│   └── modules/{gcs,bigquery,iam}/
├── tests/unit/                      # 108 tests incl. local Spark+Delta integration
├── Makefile · pyproject.toml · setup.cfg · .pre-commit-config.yaml
└── requirements.txt
```

---

## Setup

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pre-commit install
cp .env.example .env   # fill in; never commit real credentials
gcloud auth application-default login
```

> **Local Spark notes:** tests need Java 17 (`export JAVA_HOME=$(/usr/libexec/java_home -v 17)`). The suite pins `PYSPARK_PYTHON` to the venv interpreter automatically — bare `python3` from PATH may be too new for pyspark 3.4 workers. `spark-avro` and `delta-core` jars resolve from Maven on first run.

### Make targets

```bash
make test          # unit suite (Spark-backed tests included when Java present)
make spark-test    # explicit Spark/Delta test run
make lint          # black --check + isort --check + flake8
make typecheck     # mypy
make format        # apply black + isort
make coverage      # pytest --cov=src
make airflow-up    # local Airflow UI at :8080 (admin/admin)
```

### Terraform (one env at a time)

```bash
cd terraform
terraform init
terraform plan  -var-file=environments/staging/terraform.tfvars
terraform apply -var-file=environments/staging/terraform.tfvars
```

### Submitting jobs (local dev; Dataproc equivalents documented in module docstrings)

```bash
make bronze-submit-local    # continuous Kafka → Bronze stream
make silver-submit-local    # continuous Silver MERGE
make optimize-submit-local  # one-shot table maintenance pass
```

---

## Reference Deployment Metrics (design targets)

The business case this architecture models, from the reference deployment it replicates:

| Metric | Legacy | Platform target |
|---|---|---|
| Total Cost of Ownership | $4.2M/yr (Teradata) | $1.7M/yr (−60%) |
| Pipeline Count | 45 manual ETL | 200+ orchestrated |
| Freshness | 24h batch | ≤15 min streaming |
| Incident MTTR | 4.2 h | ~22 min (SLA sweeps + reconciliation) |
| Query p95 | 4 min | seconds (partition-pruned, Z-ordered scans) |
| Time-to-onboard-source | 6–8 weeks | days (config-as-code topics/jobs/DAG) |

---

## Roadmap

- [ ] **Phase 7 — Observability depth:** Prometheus/Grafana exporters, seasonal anomaly baselines, OpenLineage column-level lineage, SLO error budgets
- [ ] **Gold layer:** dbt models (domain marts), snapshots for SCD2 dims, slim-CI
- [ ] **Phase 8 — Release engineering:** GitHub Actions CI (lint/type/test/dag-contract/terraform-plan), staging deploys, canary patterns
- [ ] Multi-region DR drills; RTO/RPO validation

---

## License

Internal proprietary software. All rights reserved.
