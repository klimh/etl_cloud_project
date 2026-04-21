import os
import logging
import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors
from dotenv import load_dotenv

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
TABLE_ID = os.getenv("BQ_TABLE_ID")

# data validation schema
schema = pa.DataFrameSchema({
    "id": pa.Column(str, nullable=False),
    "timestamp": pa.Column(pa.DateTime, nullable=False),
    "city": pa.Column(str, nullable=False),
    "temperature": pa.Column(float, pa.Check.in_range(-50.0, 60.0), nullable=False),
    "description": pa.Column(str, nullable=True)
})

def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    
    valid_mask = pd.Series(True, index=df.index)
    
    valid_mask = valid_mask & df['timestamp'].notna()
    valid_mask = valid_mask & df['temperature'].notna()
    valid_mask = valid_mask & df['city'].notna()
    
    valid_df = df[valid_mask].copy()
    error_df = df[~valid_mask].copy()
    
    # pełna walidacja pandera na wierszach już po tych które przeszły wstępna selekcje
    try:
        valid_df = schema.validate(valid_df, lazy=True)
    except SchemaErrors as err:
        logger.warning(f"Caught schema errors in {len(err.failure_cases)} rows")
        # zbieramy indexy błednych wierszy wg pandery
        bad_indices = err.failure_cases['index'].unique()
        pandera_errors_df = valid_df.loc[bad_indices]
        
        # przenosimy błędne do error_df i usuwamy z valid_df
        error_df = pd.concat([error_df, pandera_errors_df])
        valid_df = valid_df.drop(index=bad_indices)

    return valid_df, error_df

def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    """
    Zapisywanie danych do bq przy uzyciu zoptymalizowanej tabeli (partcjonowanie i klastrowanie).
    """
    if df.empty:
        logger.info("No valid data to send.")
        return
        
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # optymalizacja tabeli bigquery 
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp" #partycjonowanie po dacie logu 
        ),
        clustering_fields=["city"], #klastrujemy po mieście dla szybszego wyszukiwania
    )
    
    logger.info(f"Sending {len(df)} rows to {table_ref}...")
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info("Data successfully loaded")
    except Exception as e:
        logger.error(f"Error during loading data to BQ: {e}")

def process_csv_in_chunks(filepath: str, chunk_size: int = 2000):
    """
    Przetworzenie dużego pliku w chunkach o stałej wartości 2000 wierszy
    Dzieki temu proces jest wydajny i nie zajmuje duzo ramu
    """
    logger.info(f"Starting to process file: {filepath}")
    
    total_valid = 0
    total_errors = 0
    error_file = "error_logs.csv"
    
    # jeśli plik z błędami istniejeto  usuwa go
    if os.path.exists(error_file):
        os.remove(error_file)
    
    # przetwarzanie pliku w pętli chunków
    try:
        for i, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
            logger.info(f"Processing chunk #{i+1}...")
            
            valid_df, error_df = validate_data(chunk)
            
            total_valid += len(valid_df)
            total_errors += len(error_df)
            
            # błędne rekordy trafiają do dead-letter file
            if not error_df.empty:
                error_df.to_csv(error_file, mode='a', header=not os.path.exists(error_file), index=False)
                logger.warning(f"Found {len(error_df)} error rows in this chunk. Saved to {error_file}")
            
            # wysyłka paczki danych do bigquery
            if not valid_df.empty:
                load_to_bigquery(valid_df, PROJECT_ID, DATASET_ID, TABLE_ID)
                
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return

    logger.info("ETL Process Completed.")
    logger.info(f"Total valid records: {total_valid}")
    logger.info(f"Total error records: {total_errors}")

if __name__ == "__main__":
    csv_file = "raw_weather_logs.csv"
    process_csv_in_chunks(csv_file, chunk_size=3000)
