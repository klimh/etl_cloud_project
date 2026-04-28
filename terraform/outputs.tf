output "gcs_bucket_url" {
  description = "GCS bucket URL for analytics reports"
  value       = "gs://${google_storage_bucket.reports_bucket.name}"
}

output "bigquery_dataset" {
  description = "Full BigQuery dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.main.dataset_id}"
}

output "bigquery_table" {
  description = "Full BigQuery table reference"
  value       = "${var.project_id}.${google_bigquery_dataset.main.dataset_id}.${google_bigquery_table.weather_logs.table_id}"
}
