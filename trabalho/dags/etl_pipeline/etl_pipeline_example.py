from airflow.decorators import dag, task # type: ignore
from datetime import datetime

from etl_pipeline.visualization import send_data_to_superset
from etl_pipeline.extract import extract_coins
from etl_pipeline.store import store_data

@dag(
    dag_id="etl_pipeline_encadeado",
    description="Pipeline ETL com chamadas encadeadas (@dag/@task)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exemplo", "etl", "encadeado"]
)
def etl_pipeline():    
    coins_extracted = extract_coins()
    stored = store_data(coins_extracted, table_name="meme_coins")
    send_data_to_superset(stored)

etl_pipeline()
