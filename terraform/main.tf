terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}


resource "google_storage_bucket" "reports_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = false 

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    env     = "production"
    project = "etl-pipeline"
  }
}

resource "google_bigquery_dataset" "main" {
  dataset_id    = var.bq_dataset_id
  friendly_name = "ETL Pipeline Dataset"
  description   = "Contains weather log data loaded by the ETL pipeline."
  location      = var.region

  labels = {
    env     = "production"
    project = "etl-pipeline"
  }

  lifecycle {
    ignore_changes = [description, friendly_name, labels]
  }
}


resource "google_bigquery_table" "weather_logs" {
  dataset_id          = google_bigquery_dataset.main.dataset_id
  table_id            = var.bq_table_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  clustering = ["city"]

  schema = jsonencode([
    {
      name = "id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "city"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "temperature"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name        = "description"
      type        = "STRING"
      mode        = "NULLABLE"
    }
  ])

  labels = {
    env     = "production"
    project = "etl-pipeline"
  }

  lifecycle {
    ignore_changes = [deletion_protection]
  }

  depends_on = [google_bigquery_dataset.main]
}
