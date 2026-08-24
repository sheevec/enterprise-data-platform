# Silver/Gold analytical dataset containers. Table partitioning/clustering is
# owned by processing code + dbt; Terraform owns the container, retention
# defaults and labels.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

resource "google_bigquery_dataset" "this" {
  for_each = var.datasets

  project                    = var.project_id
  dataset_id                 = "${each.value.dataset_id}_${var.environment}"
  location                   = var.location
  delete_contents_on_destroy = false   # never nuke data via terraform destroy

  default_table_expiration_ms = each.value.default_table_expiration_days != null ? (
    each.value.default_table_expiration_days * 86400000
  ) : null

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

output "dataset_ids" {
  value = { for k, d in google_bigquery_dataset.this : k => d.dataset_id }
}
