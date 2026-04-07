import os
import uuid
import logging
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
TABLE_ID = os.getenv("BQ_TABLE_ID")

def extract_weather_data(city: str) -> dict:
    """[EXTRACT] Pobiera dane z zewnętrznego REST API w tym przypadku pogodowego OpenWeatherMap"""
    logger.info(f"Pobieranie danych o pogodzie dla miasta: {city}...")
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=pl"
    
    response = requests.get(url)
    response.raise_for_status()
    
    raw_data = response.json()
    logger.info("Dane pobrane z API pomyślnie.")
    return raw_data

def transform_data(raw_data: dict) -> pd.DataFrame:
    """[TRANSFORM] Formatuje, czyści i dopasowuje strukturę do Google BigQuery za pomocą Pandas"""
    logger.info("Rozpoczynamy tranformacje danych....")
    
    city = raw_data.get("name")
    temp = raw_data.get("main", {}).get("temp")
    desc = raw_data.get("weather", [{}])[0].get("description", "brak danych")
    
    row = {
        "id": str(uuid.uuid4()),
        "timestamp": pd.Timestamp.now('UTC'),
        "user_action": f"Pobrano temperaturę w {city}: {temp} st.C, ulica: {desc}"
    }
    
    df = pd.DataFrame([row])
    
    df = df.drop_duplicates()
    
    logger.info(f"Transformacja zakończona. Rekord gotow do wysyłki:\n{df.to_string(index=False)}")
    return df

def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    """[LOAD] Bezpiecznie transmituje ramkę (DataFrame) do hurtowni Google"""
    logger.info(f"Nawiązywanie autoryzowanego połączenia z projektem GCP ({project_id})...")
    
    client = bigquery.Client(project=project_id)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    
    logger.info(f"Wysyłanie danych docelowo do {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    job.result()
    
    logger.info("SUKCES! Pipeline ETL zakończony. Dane zapisane w BigQuery.")

if __name__ == "__main__":
    try:
        miasto = "Warszawa"
        data_json = extract_weather_data(miasto)
        clean_df = transform_data(data_json)
        load_to_bigquery(clean_df, PROJECT_ID, DATASET_ID, TABLE_ID)
        
    except Exception as e:
        logger.error(f"ETL Pipeline natrafił na krytyczny Błąd: {e}")
