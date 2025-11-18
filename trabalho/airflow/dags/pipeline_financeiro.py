from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

# Importações dos seus scripts
from include.ingestion.download_data import download_financial_data
from include.annotate.annotate_data import annotate_financial_data
from include.validate.validate_data import validate_data
from include.store.store import store_data

default_args = {
    "owner": "morgana",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_financeiro",
    default_args=default_args,
    description="Pipeline financeiro com ingestão, anotação e validação",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["financeiro", "yfinance", "dataops"],
):

    @task
    def ingestao():
        return download_financial_data()

    @task
    def anotacao(raw_path: str):
        return annotate_financial_data(raw_path)

    @task
    def validacao(annotated_path: str):
        return validate_data(annotated_path)
    
    @task
    def store(annotated_path: str):
        return store_data(annotated_path)

    # Fluxo da DAG
    raw = ingestao()
    annotated = anotacao(raw)
    validacao(annotated)
    store(annotated)