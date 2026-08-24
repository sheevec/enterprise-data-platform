variable "bronze_bucket_name" {
  type        = string
  default     = ""
  description = "Existing bronze GCS bucket to grant bronze_consumer write access. Empty = skip."
}

variable "silver_bucket_name" {
  type        = string
  default     = ""
  description = "Curated Silver/GCS bucket for silver_processor + maintenance. Empty = skip."
}

variable "monitoring_dataset_id" {
  type        = string
  default     = ""
  description = "BigQuery dataset holding pipeline/DQ monitoring tables. Empty = skip."
}

variable "airflow_sa_email" {
  type        = string
  description = "Composer/Airflow service account allowed to impersonate workload SAs."
}
