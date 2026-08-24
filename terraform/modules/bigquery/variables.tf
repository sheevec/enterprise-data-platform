variable "project_id" {
  type = string
}

variable "environment" {
  type = string
}

variable "location" {
  type        = string
  default     = "US"
}

variable "datasets" {
  type = map(object({
    dataset_id                    = string
    default_table_expiration_days = optional(number)
  }))
}
