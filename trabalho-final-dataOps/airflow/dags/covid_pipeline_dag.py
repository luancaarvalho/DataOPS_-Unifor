import sys
import os
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.sensors.base import BaseSensorOperator

sys.path.append("/opt/airflow/include")

from ingestion import fetch_covid_data
from transform import transform_data
from load_postgres import load_to_postgres

class LocalFileSensor(BaseSensorOperator):
    template_fields = ("filepath",)

    def __init__(self, filepath: str, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath

    def poke(self, context):
        exists = os.path.exists(self.filepath)

        if not exists and context["ti"].try_number == 1:
            self.log.info(
                f"Primeira execução detectada. Arquivo ainda não existe, "
                f"mas seguiremos para criar via ingest_data()."
            )
            return True

        if exists:
            self.log.info(f"Arquivo encontrado: {self.filepath}")
        else:
            self.log.info(f"Aguardando arquivo: {self.filepath}")

        return exists

RAW_DATASET = Dataset("/opt/airflow/data/raw/covid_data.csv")

@dag(
    dag_id="covid_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["covid", "dataops", "endtoend"],
)
def covid_pipeline():
    wait_for_data = LocalFileSensor(
        task_id="wait_for_data",
        filepath="/opt/airflow/data/raw/covid_data.csv",
        poke_interval=60,
        timeout=600,
    )

    @task(outlets=[RAW_DATASET])
    def ingest_data():
        fetch_covid_data()

    @task()
    def validate_data():
        df = pd.read_csv("/opt/airflow/data/raw/covid_data.csv")
        assert not df.isnull().values.any(), "Há valores nulos!"
        assert (df["confirmed"] >= 0).all(), "Casos negativos encontrados!"
        print("Dados validados com sucesso!")

    @task()
    def transform_and_load():
        df = transform_data()
        load_to_postgres(df)

    @task()
    def notify_grafana():
        print("Dados atualizados e disponíveis no Grafana em tempo real!")

    (
        wait_for_data
        >> ingest_data()
        >> validate_data()
        >> transform_and_load()
        >> notify_grafana()
    )

covid_pipeline()
