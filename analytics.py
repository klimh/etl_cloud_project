import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

def generate_report():
    load_dotenv()
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_TABLE_ID")
    
    if not all([project_id, dataset_id, table_id]):
        print("Missing GCP credentials in .env")
        return

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # query to get some basic stats per city
    query = f"""
        SELECT 
            city,
            COUNT(*) as record_count,
            ROUND(AVG(temperature), 2) as avg_temp,
            ROUND(MAX(temperature), 2) as max_temp,
            ROUND(MIN(temperature), 2) as min_temp
        FROM `{table_ref}`
        GROUP BY city
        ORDER BY avg_temp DESC
    """
    
    print(f"Running aggregation query on {table_ref}...")
    try:
        df = client.query(query).to_dataframe()
        
        if df.empty:
            print("No data found in the table.")
            return

        print("\n=== WEATHER REPORT BY CITY ===")
        print(df.to_string(index=False))
        
        # save report to disk
        report_file = "weather_report.csv"
        df.to_csv(report_file, index=False)
        print(f"\nReport saved to {report_file}")
        
    except Exception as e:
        print(f"Query failed: {e}")

if __name__ == "__main__":
    generate_report()
