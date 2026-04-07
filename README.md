# ETL Pipeline to Google BigQuery

A lightweight, robust Data Engineering ETL (Extract, Transform, Load) pipeline implemented in Python. It extracts live data from a REST API, transforms and cleans it using `pandas`, and securely streams it into a Google BigQuery data warehouse.

## Project Highlights
- **Extract**: Fetches nested JSON log/data from an external REST API (OpenWeather).
- **Transform**: Standardizes time dimensions, generates unique uuid keys, removes duplicates, and dynamically maps external payload structures to predefined BigQuery schemas.
- **Load**: Securely authenticates with Google Cloud via Service Account credential file and appends new transaction records natively.
- **Robustness**: Uses standard `logging`, `try-except` guardrails, and environment variable separation for config handling.

## Tech Stack
- **Language**: Python 3.x
- **ETL Processing**: `pandas`, `pyarrow`
- **Cloud Integration**: `google-cloud-bigquery`
- **Platform**: Google Cloud Platform (Data Warehouse)

## Security & Best Practices
- **No Hardcoded Credentials**: API Keys, Target IDs and paths are exclusively pulled from `.env`.
- **Service Account Identity**: Data streaming relies on `klucz_gcp.json` strictly excluded from version control via `.gitignore`.
- **Modular Stages**: Separated `extract()`, `transform()`, and `load()` methods making the pipeline extremely scalable and easy to test.

## How to run
1. Set up a destination Dataset and Table in BigQuery.
2. Install Python dependencies: `pip install -r requirements.txt`.
3. Provide your `.env` (refer to `.env.example`).
4. Execute: `python main.py`
