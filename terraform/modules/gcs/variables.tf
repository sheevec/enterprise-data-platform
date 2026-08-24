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

variable "labels" {
  type        = map(string)
  default     = {}
  description = "Additional labels applied to every bucket."
}

variable "buckets" {
  type = map(object({
    name         = string
    versioning   = optional(bool, true)
    nearline_age = optional(number, 30)
    coldline_age = optional(number, 90)
  }))
}
