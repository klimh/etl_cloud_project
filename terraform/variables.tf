variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for GCS bucket and provider"
  type        = string
  default     = "europe-central2" 
}


variable "credentials_file" {
  description = "Path to GCP service account JSON key file"
  type        = string
  default     = "../klucz_gcp.json"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for analytics reports - must be unique globally"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "bq_table_id" {
  description = "BigQuery table ID for weather logs"
  type        = string
}
