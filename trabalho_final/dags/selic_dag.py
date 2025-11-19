from airflow.decorators import dag, task # type: ignore
from datetime import datetime
from pathlib import Path

import duckdb
import requests
import pandas as pd

@task
def fetch_selic_data(date: str= ''):
    print("Fecth SELIC data")

    if not date:
        date = "{{ ds }}" 
    
    print(f"Collecting SELIC data for date: {date}")
    date = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")

    
    BCB_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
    SELIC_CODE = 11

    url = f"{BCB_API_URL}.{SELIC_CODE}/dados?formato=json&dataInicial={date}&dataFinal={date}"

    print(f"📄 Fetching API: {url}")

    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception("Failed to get BCB data")
    
    print(f"Got successful response!")

    return pd.DataFrame(resp.json())

def duckdb_connect():
    current_dir = Path(__file__).parent
    relative_db_path = Path("../../data/financial_data.duckdb")
    path = (current_dir / relative_db_path).resolve()

    print(f"Connecting to duckDB at {path}...")

    return duckdb.connect(path)

@task
def store_selic_data(df):
    print("Store SELIC data")

    table = "selic"
    temp_table = "temp_selic"

    conn = duckdb_connect()

    print(f"Loading dataframe to duckDB: {table}...")
    
    conn.register(temp_table, df)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {temp_table}")

    conn.execute(f"INSERT INTO {table} SELECT * FROM {temp_table}")

    print("Succesfully loaded data to duckDB!")

    print("Closing dependencies...")
    conn.close()


@dag(
    dag_id="selic_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@daily",
    tags=["finance"],
)
def pipeline():
    print(f"Starting ETL pipeline")

    df = fetch_selic_data()
    store_selic_data(df)

    print(f"Finished!")

# trigger dag
pipeline()
