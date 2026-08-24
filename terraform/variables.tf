variable "project_id" {
  type        = string
  description = "GCP project hosting the platform."
}

variable "environment" {
  type        = string
  description = "staging | prod — suffixed onto every resource name."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "data_classification" {
  type    = string
  default = "confidential-pii"
}

variable "bronze_bucket_base" {
  type    = string
  default = "edp-bronze-raw"
}

variable "silver_bucket_base" {
  type    = string
  default = "edp-silver-curated"
}

variable "dq_reports_bucket_base" {
  type    = string
  default = "edp-dq-reports"
}

variable "airflow_sa_email" {
  type        = string
  description = "Composer/Airflow SA permitted to impersonate workload SAs."
}
