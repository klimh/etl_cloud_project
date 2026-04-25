# Big Data ETL Pipeline to Google BigQuery

A robust Data Engineering ETL (Extract, Transform, Load) pipeline implemented in Python. It processes large-scale CSV data (such as offline weather or user action logs), validates it using rigorous schemas, and securely loads it into a highly-optimized Google BigQuery data warehouse.

*Note: The original real-time API extraction pipeline using OpenWeatherMap is preserved in `api_v1_pipeline.py`.*

## Project Highlights
- **Extract (Large-Scale Processing)**: Reads large CSV datasets (`raw_weather_logs.csv`) using Pandas chunking (`chunksize`), effectively managing memory during high-volume data ingestion.
- **Transform & Data Validation**: Uses `pandera` to enforce strict data schemas. Invalid data (e.g., incorrect temperature types, missing critical fields) is intercepted, logged, and isolated to a dead-letter file (`error_logs.csv`), ensuring only clean data reaches the warehouse.
- **Load & Optimization**: Securely streams valid chunks into BigQuery. The target table is **optimized** upon creation with **Time Partitioning** (by timestamp) and **Clustering** (by city) to significantly reduce query costs and increase analytical performance.
- **Analyze & Export**: Includes `analytics.py` script that connects back to BigQuery to run SQL aggregations (averages, max/min temperatures per city) and exports a final business report. The report is automatically pushed to a **Google Cloud Storage (GCS)** Data Lake bucket, demonstrating an end-to-end data lifecycle.
- **Containerized**: The entire pipeline runs inside a Docker container — no local Python environment required. A multi-stage `Dockerfile` keeps the image lean, and `docker-compose.yml` orchestrates both pipeline steps.
- **Robustness**: Employs extensive `logging`, structured `try-except` guardrails, and environment variable separation for configuration handling.

## Tech Stack
- **Language**: Python 3.11
- **ETL Processing**: `pandas`, `pyarrow`
- **Data Validation**: `pandera`
- **Cloud Integration**: `google-cloud-bigquery`, `google-cloud-storage`
- **Containerization**: Docker, Docker Compose
- **Platform**: Google Cloud Platform (BigQuery + Cloud Storage)

## Security & Best Practices
- **No Hardcoded Credentials**: Target IDs and paths are pulled from `.env`.
- **Credentials Never in Image**: The GCP service account key is mounted into the container at runtime via a Docker volume — it is never copied into the image.
- **Error Handling**: Malformed data rows do not crash the pipeline but are elegantly skipped and logged for further analysis.

## How to Run

### Option A — Docker (recommended)

> Prerequisites: Docker Desktop installed and running.

```bash
# 1. Copy and fill in your GCP config
cp .env.example .env

# 2. Generate the dummy dataset
python generate_data.py

# 3. Build the image and run the full pipeline (ETL → Analytics)
docker compose up --build

# Run only the ETL step
docker compose run etl

# Run only the analytics/reporting step
docker compose run analytics
```

Error logs are written back to `error_logs.csv` and the report to `weather_report.csv` on your host machine via volume mounts.

### Option B — Local Python

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy and fill in your GCP config
cp .env.example .env

# 3. Generate dummy data
python generate_data.py

# 4. Run ETL
python main.py

# 5. Run analytics
python analytics.py
```
