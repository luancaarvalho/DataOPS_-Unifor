from airflow.decorators import dag, task # type: ignore
from airflow.operators.python import get_current_context # type: ignore
from datetime import datetime
from pathlib import Path
import os
import random

import duckdb
import requests
import pandas as pd

@task
def fetch_selic_data():
    print("Fecth SELIC data")

    context = get_current_context()
    dt = context["logical_date"]
    
    print(f"Collecting SELIC data for date: {dt}")

    BCB_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
    SELIC_CODE = 11

    url = f"{BCB_API_URL}.{SELIC_CODE}/dados?formato=json&dataInicial={dt.strftime('%d/%m/%Y')}&dataFinal={dt.strftime('%d/%m/%Y')}"
    print(f"📄 Fetching API: {url}")

    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"Failed to get BCB data: {resp.status_code} - {resp.text}")
    
    print(f"Got successful response!")

    return pd.DataFrame(resp.json())

def duckdb_connect():
    path = "/opt/airflow/data/"

    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    db_path = Path(path) / "finance.duckdb"
    conn = duckdb.connect(str(db_path))

    print(f"Connecting to duckDB at {db_path}...")

    return conn

@task
def store_selic_data(df, table):
    with duckdb_connect() as conn:
        print(f"Store {table} data")

        temp_table = f"temp_{table}"

        print(f"Loading dataframe to duckDB: {table}...")
        conn.register(temp_table, df)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM {temp_table}")

        conn.execute(f"INSERT INTO {table} SELECT * FROM {temp_table}")

        print("Succesfully loaded data to duckDB!")

        print("Closing dependencies...")
        conn.close()

@task
def annotate_selic_data(df):
    print("Annotate SELIC data")
    # 20% "great selic value", 80% "omg, Brasil will blow up"
    annotations = [
        "great selic value" if random.random() < 0.2 else "omg, Brasil will blow up"
        for _ in range(len(df))
    ]
    df["annotation"] = annotations
    return df

@dag(
    dag_id="selic_dag",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    schedule="@monthly",
    tags=["finance"],
)
def pipeline():
    print(f"Starting ETL pipeline")

    df = fetch_selic_data()
    df = annotate_selic_data(df)
    store_selic_data(df, "selic")

    print(f"Finished!")

# trigger dag
pipeline()
