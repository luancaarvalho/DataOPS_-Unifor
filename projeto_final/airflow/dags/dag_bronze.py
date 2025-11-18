# -*- coding: utf-8 -*-
"""
DAG Bronze — Ingestão de API → Camada Bronze (MinIO)
-----------------------------------------------------
Fluxo:
1. Extrai dados de uma API usando ApiHandler (com retry + logging).
2. Salva localmente em /data/bronze.
3. Faz upload automático para o bucket 'dataops-bronze' no MinIO.
"""

import os
from datetime import datetime

from core.api import ApiHandler
from core.minio_handler import upload_file


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# =====================================================
# 🔧 CONFIGURAÇÕES
# =====================================================
LOCAL_PATH = "/opt/airflow/data/bronze"
BUCKET_NAME = "bronze"


# =====================================================
# 🧠 FUNÇÕES DA DAG
# =====================================================
def extract_and_save_api(**context):
    """Extrai dados da API e salva localmente"""
    os.makedirs(LOCAL_PATH, exist_ok=True)
    api = ApiHandler()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(LOCAL_PATH, f"bronze_dataset_{timestamp}.csv")

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            for line in api.get_lines():
                f.write(line + "\n")

        context["ti"].xcom_push(key="bronze_file_path", value=file_path)
        print(f"✅ Arquivo salvo localmente em {file_path}")

    except Exception as err:
        print(f"❌ Falha na extração da API: {err}")
        raise err


def upload_bronze_to_minio(**context):
    """Envia o arquivo CSV da camada bronze para o MinIO"""
    from minio.error import S3Error

    file_path = context["ti"].xcom_pull(key="bronze_file_path")

    try:
        upload_file(
            local_path=file_path,
            bucket_name=BUCKET_NAME,
            object_name=os.path.basename(file_path),
        )
        print(f"✅ Arquivo enviado para MinIO: {file_path}")

    except S3Error as err:
        print(f"❌ Erro no upload para MinIO: {err}")
        raise


def log_success():
    print("🎯 Camada Bronze concluída com sucesso! Dados disponíveis no MinIO.")


# =====================================================
# 🧭 DEFINIÇÃO DA DAG
# =====================================================
with DAG(
    dag_id="bronze",
    description="Ingestão da API → Camada Bronze (MinIO)",
    start_date=datetime(2025, 11, 8),
    schedule_interval=None,  # Execução manual
    catchup=False,
    is_paused_upon_creation=False,
    tags=["bronze", "api", "minio"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_save_api",
        python_callable=extract_and_save_api,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_bronze_to_minio",
        python_callable=upload_bronze_to_minio,
        provide_context=True,
    )

    success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )
    
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_layer",
        trigger_dag_id="silver",
        conf={
            "bronze_file_path": "{{ ti.xcom_pull(key='bronze_file_path') }}"
        },
    )

    extract_task >> upload_task >> success_task >> trigger_silver
