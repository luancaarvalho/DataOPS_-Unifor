from __future__ import annotations

from importlib.abc import Loader
from logging import Logger

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from trabalho_final.src.tasks.extractor import Extractor
from trabalho_final.src.tasks.loader import Loader

log = Logger("dags.selic")

def _fetch_selic_data(date, **kwargs):
    log.logger.info("Starting ETL pipeline")

    extractor = Extractor()
    loader = Loader()

    df = extractor.collect_selic(date)
    loader.load_selic_history(df)

    log.logger.info("ETL pipeline finished successfully!")

    log.logger.info("Closing dependencies...")
    loader.close()

with DAG(
    dag_id="selic_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["finance"],
) as dag:
     
    date_str = "{{ ds }}"

    fetch_selic_data_task = PythonOperator(
        task_id="fetch_selic_data",
        python_callable=_fetch_selic_data,
        op_kwargs={"date": date_str},
    )

