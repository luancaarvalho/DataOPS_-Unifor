# -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ===============================
# CONFIGS
# ===============================
LOCAL_SILVER = "/opt/airflow/data/silver"
SILVER_BUCKET = "silver"


def fetch_bronze_file_from_trigger(**context):
    """
    Recebe o caminho local gerado pela Bronze via TriggerDagRunOperator.
    NÃO baixa nada do MinIO. Usa o arquivo local da Bronze.
    """

    os.makedirs(LOCAL_SILVER, exist_ok=True)

    bronze_file_path = context["dag_run"].conf.get("bronze_file_path")

    if not bronze_file_path:
        raise ValueError("bronze_file_path não recebido na DAG Silver!")

    if not os.path.exists(bronze_file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {bronze_file_path}")

    print(f"📄 Usando arquivo Bronze local: {bronze_file_path}")

    # Envia para o próximo task
    context["ti"].xcom_push(key="bronze_local_path", value=bronze_file_path)


def transform_to_silver(**context):
    from core.minio_handler import ensure_bucket, upload_file

    os.makedirs(LOCAL_SILVER, exist_ok=True)

    bronze_path = context["ti"].xcom_pull(key="bronze_local_path")
    if not bronze_path:
        raise ValueError("bronze_local_path não encontrado no XCom!")
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {bronze_path}")

    df = pd.read_csv(bronze_path)

    # =====================================================
    # 🔹 1) Normalização básica
    # =====================================================
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.drop_duplicates()
    df = df.fillna("")

    # =====================================================
    # 🔹 2) Padronização de education_level
    # =====================================================
    df["education_level"] = df["education"].replace({
        "university.degree": "university",
        "high.school": "high_school",
        "professional.course": "professional",
        "basic.4y": "basic",
        "basic.6y": "basic",
        "basic.9y": "basic",
        "illiterate": "illiterate",
        "unknown": pd.NA,
    })

    # =====================================================
    # 🔹 3) LIMPEZA DE VALORES INVÁLIDOS (Silver)
    # =====================================================
    # Converter tipos numéricos (evita erro de string)
    num_cols = ["age", "duration", "campaign", "pdays", "previous"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Idade inválida (<18 ou >100) → NA
    if "age" in df.columns:
        df.loc[(df["age"] < 18) | (df["age"] > 100), "age"] = pd.NA

    # Valores negativos em duration, campaign, pdays, previous → 0
    for col in ["duration", "campaign", "pdays", "previous"]:
        if col in df.columns:
            df[col] = df[col].clip(lower=0)

    # Strings indesejadas para categorias → NA
    df.replace({"unknown": pd.NA, "?": ""}, inplace=True)

    # =====================================================
    # 🔹 4) GERAR SILVER (PARQUET)
    # =====================================================
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_name = f"silver_dataset_{timestamp}.parquet"
    silver_path = os.path.join(LOCAL_SILVER, silver_name)

    df.to_parquet(silver_path, index=False)
    print(f"💾 Silver salvo localmente: {silver_path}")

    # XCom com caminho completo
    context["ti"].xcom_push(key="silver_parquet_path", value=silver_path)

    # Enviar ao MinIO
    ensure_bucket(SILVER_BUCKET)
    upload_file(silver_path, SILVER_BUCKET, silver_name)

    print(f"📤 Silver enviado ao MinIO: {silver_name}")


def log_success():
    print("🎯 Camada Silver concluída com sucesso!")


# ===============================
# DEFINIÇÃO DA DAG
# ===============================
with DAG(
    dag_id="silver",
    description="Transformação da camada Bronze → Silver (Bank Marketing)",
    start_date=datetime(2025, 11, 13),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["silver", "minio"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_bronze_file_from_trigger",
        python_callable=fetch_bronze_file_from_trigger,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
        provide_context=True,
    )

    success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    # ========================================
    # 🔥 NOVO: disparo automático da GOLD
    # ========================================
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_layer",
        trigger_dag_id="gold",
        conf={
            "silver_file_path": "{{ ti.xcom_pull(key='silver_parquet_path') }}"
        },
    )

    fetch_task >> transform_task >> success_task >> trigger_gold
