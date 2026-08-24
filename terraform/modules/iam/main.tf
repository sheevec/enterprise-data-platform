# Least-privilege service accounts for the data platform.
# Each workload gets its own SA; permissions are granted on the SPECIFIC
# resources it touches (bucket/dataset-level), never wildcard project admin.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  type = string
}

variable "environment" {
  type = string
}

locals {
  services = {
    bronze_consumer  = "Bronze Kafka->GCS ingestion (streaming consumer / Spark streaming job)"
    silver_processor = "Bronze->Silver Spark MERGE jobs"
    maintenance      = "OPTIMIZE/Z-ORDER/VACUUM + GDPR erasure runner"
    airflow          = "Orchestrator submitting Dataproc/K8s jobs"
    dq_monitor       = "Data quality gate: read tables, write monitoring dataset"
  }
}

resource "google_service_account" "workload" {
  for_each = local.services

  account_id   = "edp-${each.key}-${var.environment}"
  display_name = "[EDP ${var.environment}] ${each.value}"
}

# --- Bronze consumer: objectAdmin ONLY on the bronze bucket -------------------
resource "google_storage_bucket_iam_member" "bronze_writer" {
  count = var.bronze_bucket_name == "" ? 0 : 1

  bucket = var.bronze_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workload["bronze_consumer"].email}"
}

# --- Silver/maintenance: objectAdmin on curated buckets -----------------------
resource "google_storage_bucket_iam_member" "silver_writer" {
  count = var.silver_bucket_name == "" ? 0 : 1

  bucket = var.silver_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workload["silver_processor"].email}"
}

resource "google_storage_bucket_iam_member" "maintenance_writer" {
  count = var.silver_bucket_name == "" ? 0 : 1

  bucket = var.silver_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.workload["maintenance"].email}"
}

# --- BigQuery: dataset-scoped editor for DQ monitor, jobUser at project -------
resource "google_bigquery_dataset_iam_member" "dq_monitor_editor" {
  count = var.monitoring_dataset_id == "" ? 0 : 1

  dataset_id = var.monitoring_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.workload["dq_monitor"].email}"
}

resource "google_project_iam_member" "bq_job_user" {
  for_each = local.services

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.workload[each.key].email}"
}

# --- Airflow may impersonate every workload SA (no key sharing) ---------------
resource "google_service_account_iam_member" "airflow_impersonation" {
  for_each = local.services

  service_account_id = google_service_account.workload[each.key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${var.airflow_sa_email}"
}

output "service_account_emails" {
  description = "Map of service name -> SA email. Wire into job specs via Workload Identity."
  value       = { for k, sa in google_service_account.workload : k => sa.email }
}
