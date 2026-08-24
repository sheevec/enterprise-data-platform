# One environment per apply. From terraform/:
#   terraform init
#   terraform plan  -var-file=environments/staging/terraform.tfvars
#   terraform apply -var-file=environments/staging/terraform.tfvars
#
# State backend (uncomment + set bucket once per env):
# terraform {
#   backend "gcs" {
#     prefix = "edp/terraform"
#   }
# }

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  common_labels = {
    data_classification = var.data_classification
    managed_by          = "terraform"
  }
}

module "gcs" {
  source = "./modules/gcs"

  project_id  = var.project_id
  environment = var.environment
  location    = var.bq_location

  labels = local.common_labels

  buckets = {
    bronze = {
      name         = var.bronze_bucket_base
      versioning   = true
      nearline_age = 30
      coldline_age = 90
    }
    silver = {
      name         = var.silver_bucket_base
      versioning   = true
      nearline_age = 60
      coldline_age = 180
    }
    dq_reports = {
      name         = var.dq_reports_bucket_base
      versioning   = false
      nearline_age = 14
      coldline_age = 60
    }
  }
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  environment = var.environment
  location    = var.bq_location

  datasets = {
    monitoring = {
      dataset_id                    = "pipeline_monitoring"
      default_table_expiration_days = null
    }
    dq_scores = {
      dataset_id                    = "data_quality_monitoring"
      default_table_expiration_days = 365
    }
    gold_marts = {
      dataset_id                    = "gold"
      default_table_expiration_days = null
    }
  }
}

module "iam" {
  source = "./modules/iam"

  project_id            = var.project_id
  environment           = var.environment
  airflow_sa_email      = var.airflow_sa_email
  bronze_bucket_name    = module.gcs.bucket_names["bronze"]
  silver_bucket_name    = module.gcs.bucket_names["silver"]
  monitoring_dataset_id = module.bigquery.dataset_ids["monitoring"]
}

output "bronze_bucket" {
  value = module.gcs.bucket_names["bronze"]
}

output "silver_bucket" {
  value = module.gcs.bucket_names["silver"]
}

output "workload_service_accounts" {
  value = module.iam.service_account_emails
}
