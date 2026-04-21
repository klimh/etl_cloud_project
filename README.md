# Big Data ETL Pipeline to Google BigQuery

A robust Data Engineering ETL (Extract, Transform, Load) pipeline implemented in Python. It processes large-scale CSV data (such as offline weather or user action logs), validates it using rigorous schemas, and securely loads it into a highly-optimized Google BigQuery data warehouse.

*Note: The original real-time API extraction pipeline using OpenWeatherMap is preserved in `api_v1_pipeline.py`.*

## Project Highlights
- **Extract (Large-Scale Processing)**: Reads large CSV datasets (`raw_weather_logs.csv`) using Pandas chunking (`chunksize`), effectively managing memory during high-volume data ingestion.
- **Transform & Data Validation**: Uses `pandera` to enforce strict data schemas. Invalid data (e.g., incorrect temperature types, missing critical fields) is intercepted, logged, and isolated to a dead-letter file (`error_logs.csv`), ensuring only clean data reaches the warehouse.
- **Load & Optimization**: Securely streams valid chunks into BigQuery. The target table is **optimized** upon creation with **Time Partitioning** (by timestamp) and **Clustering** (by city) to significantly reduce query costs and increase analytical performance.
- **Analyze**: Includes `analytics.py` script that connects back to BigQuery to run SQL aggregations (averages, max/min temperatures per city) and export a final business report.
- **Robustness**: Employs extensive `logging`, structured `try-except` guardrails, and environment variable separation for configuration handling.

## Tech Stack
- **Language**: Python 3.x
- **ETL Processing**: `pandas`, `pyarrow`
- **Data Validation**: `pandera`
- **Cloud Integration**: `google-cloud-bigquery`
- **Platform**: Google Cloud Platform (Data Warehouse)

## Security & Best Practices
- **No Hardcoded Credentials**: Target IDs and paths are pulled from `.env`.
- **Service Account Identity**: Data streaming relies on `klucz_gcp.json` strictly excluded from version control via `.gitignore`.
- **Error Handling**: Malformed data rows do not crash the pipeline but are elegantly skipped and logged for further analysis.

## How to Run
1. Set up a destination Dataset and Table in BigQuery.
2. Install Python dependencies: `pip install -r requirements.txt`.
3. Provide your `.env` (refer to `.env.example`).
4. Generate the dummy dataset (simulating raw data): `python generate_data.py`
5. Execute the ETL pipeline: `python main.py`
6. Run the analytics step: `python analytics.py`
