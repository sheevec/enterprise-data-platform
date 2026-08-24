# Bronze + curated buckets with PB-scale lifecycle economics:
# Standard -> Nearline (default 30d) -> Coldline (default 90d).
# Versioning protects against accidental overwrite; UBLA enforces IAM-only
# access (no legacy ACLs); public access is hard-prevented.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

resource "google_storage_bucket" "this" {
  for_each = var.buckets

  name          = "${each.value.name}-${var.environment}"
  project       = var.project_id
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = each.value.versioning
  }

  lifecycle_rule {
    condition {
      age = each.value.nearline_age
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = each.value.coldline_age
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = merge(var.labels, {
    environment = var.environment
    managed_by  = "terraform"
    layer       = each.key
  })
}

output "bucket_names" {
  value = { for k, b in google_storage_bucket.this : k => b.name }
}
