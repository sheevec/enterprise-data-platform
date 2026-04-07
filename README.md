# Enterprise Data Platform

A production-grade, multi-cloud data lakehouse built for a large financial services firm, managing 500TB of data across 200+ pipelines with 99.9% uptime SLA. The platform replaced a legacy on-premises data warehouse, delivering a 60% cost reduction while expanding data access to 40+ business teams across Risk, Compliance, Trading, and Operations.

---

## Overview

The Enterprise Data Platform is a fully managed, cloud-native lakehouse built on the Medallion architecture pattern. It unifies batch and streaming data ingestion, enforces enterprise-grade data quality and governance, and exposes curated, business-ready datasets through a self-serve analytics layer. The platform is deployed across GCP (primary) and AWS (DR/burst) using Terraform-managed infrastructure.

**At a glance:**

- 500TB of managed data across structured, semi-structured, and unstructured sources
- 200+ orchestrated data pipelines running on Apache Airflow
- 99.9% uptime SLA with automated incident detection and PagerDuty alerting
- 60% reduction in total cost of ownership vs. the legacy Teradata warehouse
- 40+ business teams consuming data through BigQuery, Looker, and direct API access
- Real-time streaming ingestion via Apache Kafka at up to 2M events/second

---

## Architecture

The platform is built on the **Medallion Architecture**, a layered data organization pattern that progressively refines raw data into business-ready assets.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│  Core Banking │ Trading Systems │ Market Data │ CRM │ External APIs  │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Kafka / Batch Ingestion
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw / Immutable)                   │
│  • GCS / S3 object storage (Delta Lake format)                       │
│  • Schema-on-read, full history retained                             │
│  • Partitioned by source / date / hour                               │
│  • Avro → Parquet via Spark Structured Streaming                     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark + Great Expectations
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleansed / Conformed)              │
│  • Delta Lake tables with schema enforcement                         │
│  • Deduplication, null handling, type casting                        │
│  • PII tokenization and masking enforced                             │
│  • Row-level data quality scores applied                             │
│  • Change Data Capture (CDC) merge patterns                          │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt transformations
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (Business / Aggregated)               │
│  • Domain-oriented data marts (Risk, Finance, Operations, Trading)   │
│  • dbt-managed dimensional models and aggregations                   │
│  • Optimized for BI tools (Looker, Tableau, Power BI)               │
│  • SLA-backed freshness guarantees per domain                        │
│  • Column-level lineage tracked via OpenLineage                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
      BigQuery Analytics         Self-Serve APIs
      (Looker / Tableau)      (FastAPI / gRPC endpoints)
```

### Key Architectural Decisions

- **Delta Lake** as the open table format provides ACID transactions, time travel, and schema evolution without vendor lock-in.
- **Apache Kafka** with Confluent Schema Registry ensures schema compatibility across 50+ producing systems.
- **dbt** manages all Silver-to-Gold transformations with full lineage, testing, and documentation baked in.
- **Terraform** provisions all infrastructure across GCP and AWS, enabling reproducible, auditable deployments.
- **Monte Carlo** provides end-to-end data observability with ML-driven anomaly detection on top of our custom pipeline monitoring.

---

## Key Results

| Metric | Before | After | Improvement |
|---|---|---|---|
| Total Cost of Ownership | $4.2M / year | $1.7M / year | **60% reduction** |
| Data Pipeline Count | 45 manual ETL jobs | 200+ orchestrated pipelines | **4.4x increase** |
| Uptime / Availability | 97.2% | 99.9% | **+2.7 pp** |
| Data Freshness (avg) | 24 hours | 15 minutes (streaming) | **96x improvement** |
| Business Teams Served | 8 teams | 40+ teams | **5x increase** |
| Data Volume Managed | 80TB | 500TB | **6.25x growth** |
| Incident MTTR | 4.2 hours | 22 minutes | **91% reduction** |
| Data Quality Score | ~72% (estimated) | 98.4% (tracked) | **+26 pp** |
| Query Performance (p95) | 4 min (Teradata) | 12 sec (BigQuery) | **20x improvement** |
| Time-to-Data (new source) | 6-8 weeks | 3-5 days | **87% reduction** |

---

## Tech Stack

| Category | Technology | Purpose |
|---|---|---|
| Compute | Apache Spark 3.4 (Dataproc) | Batch and streaming data processing |
| Table Format | Delta Lake 2.4 | ACID transactions, time travel, schema evolution |
| Transformation | dbt 1.7 (BigQuery adapter) | Silver-to-Gold SQL transformations with lineage |
| Orchestration | Apache Airflow 2.8 | DAG-based pipeline scheduling and monitoring |
| Streaming | Apache Kafka (Confluent Cloud) | Real-time event ingestion and delivery |
| Schema Registry | Confluent Schema Registry | Avro schema management and compatibility |
| Data Quality | Great Expectations 0.18 | Expectation-based data validation |
| Observability | Monte Carlo | ML-driven data observability and anomaly detection |
| Storage (Primary) | Google Cloud Storage | Bronze layer object storage |
| Warehouse | Google BigQuery | Silver/Gold analytical query layer |
| Storage (DR) | AWS S3 | Disaster recovery and burst compute |
| IaC | Terraform | Multi-cloud infrastructure provisioning |
| Governance | Dataplex + custom lineage | Data cataloging and access control |
| Alerting | PagerDuty | On-call incident management |
| CI/CD | GitHub Actions | Automated testing and deployment |
| Secrets | HashiCorp Vault | Credential management |
| Containerization | Docker + Kubernetes (GKE) | Workload containerization and scaling |

---

## Data Quality Framework

Data quality is enforced at every layer using Great Expectations, with a tiered alerting model:

### Validation Tiers

| Tier | Layer | Threshold | Action on Failure |
|---|---|---|---|
| Critical | Bronze → Silver | < 95% pass rate | Block pipeline, page on-call |
| High | Silver → Gold | < 98% pass rate | Block promotion, Slack alert |
| Medium | Gold | < 99% pass rate | Warning alert, log incident |
| Monitoring | All layers | Trend-based | Weekly quality report |

### Expectation Categories

- **Completeness**: Null checks on required fields, row count expectations vs. source
- **Uniqueness**: Primary key deduplication, composite key constraints
- **Validity**: Value range checks, regex pattern matching, referential integrity
- **Timeliness**: Data freshness assertions, SLA breach detection
- **Consistency**: Cross-table reconciliation, balance checks for financial data

### Quality Scoring

Every dataset receives a **Data Quality Score (DQS)** computed as a weighted average of expectation results. Scores are persisted to a BigQuery monitoring dataset and surfaced in the internal data catalog. Datasets below threshold are flagged and hidden from the self-serve layer until remediated.

---

## Cost Optimization

The 60% cost reduction was achieved through several complementary strategies:

### Storage Optimization
- **Delta Lake Z-ordering** on high-cardinality filter columns reduced scan costs by ~35%
- **Automated table optimization** runs nightly to compact small files (target: 128MB Parquet files)
- **Tiered storage lifecycle policies**: data moves from Standard → Nearline → Coldline based on access patterns
- **Compression**: All tables use Snappy compression; columnar Parquet format reduces storage vs. row-based formats by 4-8x

### Compute Optimization
- **Autoscaling Dataproc clusters** with preemptible/spot VMs for non-critical batch jobs (70% of compute at 60-80% discount)
- **Workload scheduling**: Cost-intensive jobs run during off-peak hours (12am-6am) for sustained use discounts
- **BigQuery slot reservations** for predictable workloads vs. on-demand pricing for ad-hoc queries
- **Spark query optimization**: Broadcast joins, partition pruning, and predicate pushdown reduced shuffle-heavy job runtimes by 45%

### Architectural Savings
- Eliminated 6 redundant data marts maintained by individual teams
- Consolidated 3 separate BI tool licenses into a single Looker enterprise contract
- Decommissioned on-premises Teradata cluster ($1.8M/year license + $600K hardware maintenance)

---

## Data Governance

### Access Control
- **Column-level security** in BigQuery enforces PII masking per team role
- **Row-level security** policies restrict trading desk data to authorized users
- **Dynamic data masking**: SSN, account numbers, and card data masked for non-privileged roles
- All access governed by **Google IAM** with least-privilege principles; reviewed quarterly

### Lineage
- **OpenLineage** integrated with Airflow and dbt captures column-level lineage automatically
- Lineage graph surfaced in internal data catalog (built on DataHub)
- Impact analysis available for any upstream schema change

### Compliance
- **GDPR right-to-erasure**: Automated deletion propagation across Bronze/Silver/Gold for EU data subjects
- **SOX controls**: Immutable audit logs for all financial data transformations; 7-year retention
- **Data retention policies** enforced via automated lifecycle management, auditable via Terraform state

### Data Contracts
- Every data product exposes a versioned **data contract** (schema, SLAs, owner, quality thresholds)
- Contract violations trigger automated alerts to the data product owner
- Enforced at the Silver → Gold promotion boundary

---

## Project Structure

```
enterprise-data-platform/
├── README.md
├── requirements.txt
│
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── kafka_consumer.py          # Kafka → GCS Bronze layer consumer
│   │   ├── batch_ingestion.py         # GCS/S3 batch source connectors
│   │   └── schema_registry.py         # Confluent Schema Registry client
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── bronze_to_silver.py        # PySpark Bronze → Silver transforms
│   │   ├── cdc_processor.py           # Change Data Capture merge logic
│   │   └── pii_masking.py             # PII tokenization and masking
│   │
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── data_quality.py            # Great Expectations framework wrapper
│   │   └── expectations/
│   │       ├── bronze_suite.json      # Bronze layer expectation suites
│   │       ├── silver_suite.json      # Silver layer expectation suites
│   │       └── gold_suite.json        # Gold layer expectation suites
│   │
│   ├── observability/
│   │   ├── __init__.py
│   │   ├── pipeline_monitor.py        # Pipeline metrics, SLA, anomaly detection
│   │   └── lineage_tracker.py         # OpenLineage event emission
│   │
│   └── utils/
│       ├── __init__.py
│       ├── gcs_utils.py               # GCS helper functions
│       ├── bq_utils.py                # BigQuery helper functions
│       └── config.py                  # Environment configuration loader
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── silver/                    # Silver layer dbt models
│   │   └── gold/                      # Gold layer dbt models (domain marts)
│   │       ├── risk/
│   │       ├── finance/
│   │       ├── trading/
│   │       └── operations/
│   └── tests/                         # dbt data tests
│
├── airflow/
│   ├── dags/
│   │   ├── bronze_ingestion_dag.py    # Daily Bronze ingestion DAGs
│   │   ├── silver_processing_dag.py   # Bronze → Silver processing DAGs
│   │   └── gold_promotion_dag.py      # Silver → Gold dbt run DAGs
│   └── plugins/
│
├── terraform/
│   ├── environments/
│   │   ├── prod/
│   │   └── staging/
│   ├── modules/
│   │   ├── gcs/                       # GCS bucket definitions
│   │   ├── bigquery/                  # BigQuery dataset/table definitions
│   │   ├── dataproc/                  # Spark cluster configurations
│   │   ├── kafka/                     # Confluent Cloud provisioning
│   │   └── iam/                       # Service account and IAM bindings
│   └── main.tf
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
└── .github/
    └── workflows/
        ├── ci.yml                     # PR validation: lint, unit tests, dbt compile
        └── cd.yml                     # Deploy: Terraform plan/apply, DAG sync
```

---

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud SDK (`gcloud`) authenticated with application default credentials
- Terraform >= 1.6
- Docker (for local Airflow)
- Access to Confluent Cloud cluster (schema registry URL and API keys)

### Local Development

```bash
# Clone the repository
git clone https://github.com/your-org/enterprise-data-platform.git
cd enterprise-data-platform

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env
# Edit .env with your GCP project, Kafka bootstrap servers, etc.

# Authenticate with GCP
gcloud auth application-default login
export GOOGLE_CLOUD_PROJECT=your-project-id
```

### Infrastructure Provisioning

```bash
cd terraform/environments/staging

# Initialize Terraform
terraform init

# Review the plan
terraform plan -var-file="staging.tfvars"

# Apply (requires appropriate GCP IAM permissions)
terraform apply -var-file="staging.tfvars"
```

### Running Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires GCP credentials)
pytest tests/integration/ -v --gcp-project=your-project-id

# dbt tests (against staging BigQuery dataset)
cd dbt
dbt test --target staging
```

### Airflow Local Development

```bash
# Start Airflow via Docker Compose
docker-compose -f airflow/docker-compose.yml up -d

# Access Airflow UI at http://localhost:8080
# Default credentials: airflow / airflow
```

---

## Contributing

All contributions must pass:
1. `pre-commit` hooks (black, isort, flake8, mypy)
2. Unit test suite with > 80% coverage
3. `dbt compile` and `dbt test` on staging dataset
4. Terraform `plan` with no unexpected resource changes
5. Peer review from a member of the Data Platform team

---

## License

Internal proprietary software. All rights reserved.
