from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from include.store.metrics import log_task_metrics

# Importações dos scripts de negócio
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
    def ingestao(**kwargs):
        start_time = datetime.now()

        try:
            result = download_financial_data()

            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="success",
                start_time=start_time,
            )

            return result

        except Exception:
            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="error",
                start_time=start_time,
            )
            raise

    @task
    def anotacao(raw_path: str, **kwargs):
        start_time = datetime.now()

        try:
            result = annotate_financial_data(raw_path)

            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="success",
                start_time=start_time,
            )

            return result

        except Exception:
            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="error",
                start_time=start_time,
            )
            raise

    @task
    def validacao(annotated_path: str, **kwargs):
        start_time = datetime.now()

        try:
            result = validate_data(annotated_path)

            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="success",
                start_time=start_time,
            )

            return result

        except Exception:
            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="error",
                start_time=start_time,
            )
            raise

    @task
    def store(annotated_path: str, **kwargs):
        start_time = datetime.now()

        try:
            result = store_data(annotated_path)

            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="success",
                start_time=start_time,
            )

            return result

        except Exception:
            log_task_metrics(
                dag_id=kwargs["dag"].dag_id,
                task_id=kwargs["task"].task_id,
                run_id=kwargs["run_id"],
                status="error",
                start_time=start_time,
            )
            raise

    raw = ingestao()
    annotated = anotacao(raw)
    validacao(annotated)
    store(annotated)
