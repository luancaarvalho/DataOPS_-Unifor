# -*- coding: utf-8 -*-

import os, json, requests
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


LABEL_STUDIO_URL = "http://label-studio:8080"
LOCAL_GOLD = "/opt/airflow/data/gold"
LOCAL_ANNOT = "/opt/airflow/data/annotated"
ANNOTATED_BUCKET = "gold-annotated"


# ============================================================
# API KEY
# ============================================================

def get_labelstudio_api_key():
    conn = BaseHook.get_connection("label_studio")
    api_key = conn.extra_dejson.get("api_key")
    if not api_key:
        raise ValueError("API Key do Label Studio não encontrada!")
    return api_key


def get_headers(extra=None):
    base = {"Authorization": f"Token {get_labelstudio_api_key()}"}
    if extra:
        base.update(extra)
    return base


# ============================================================
# GOLD – pega arquivo mais recente
# ============================================================

def get_latest_gold():
    files = [f for f in os.listdir(LOCAL_GOLD) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo Gold encontrado.")
    files.sort(reverse=True)
    return os.path.join(LOCAL_GOLD, files[0])


# ============================================================
# Criar projeto com 5 classes
# ============================================================

def create_or_get_project():
    resp = requests.get(f"{LABEL_STUDIO_URL}/api/projects", headers=get_headers())
    resp.raise_for_status()
    projects = resp.json().get("results", resp.json())

    for p in projects:
        if p.get("title") == "Bank Marketing Annotation":
            print("📌 Projeto já existe:", p["id"])
            return p["id"]

    # 🔥 CONFIG CORRETA (sem erros de encoding)
    payload = {
        "title": "Bank Marketing Annotation",
        "label_config": """
<View>
    <Text name="text" value="$text" encoding="none" />
    <Choices name="label" toName="text" choice="single">
        <Choice value="altamente_interessado"/>
        <Choice value="interessado"/>
        <Choice value="neutro"/>
        <Choice value="desinteressado"/>
        <Choice value="rejeitou"/>
    </Choices>
</View>
"""
    }

    resp = requests.post(
        f"{LABEL_STUDIO_URL}/api/projects",
        headers=get_headers({"Content-Type": "application/json"}),
        data=json.dumps(payload)
    )
    resp.raise_for_status()
    return resp.json()["id"]


def clear_project_tasks(project_id):
    resp = requests.delete(
        f"{LABEL_STUDIO_URL}/api/projects/{project_id}/tasks",
        headers=get_headers()
    )
    resp.raise_for_status()
    print("🧹 Tasks antigas removidas!")


# ============================================================
# IMPORTAR GOLD → LABEL STUDIO (JSON COMPLETO)
# ============================================================

def import_gold_to_labelstudio(**context):

    project_id = create_or_get_project()
    clear_project_tasks(project_id)

    gold_path = get_latest_gold()
    df = pd.read_parquet(gold_path)

    # ID único por linha
    df["gold_key"] = df.index.astype(str)

    tasks = []

    for _, row in df.iterrows():
        record_json = json.dumps(row.to_dict(), ensure_ascii=False)

        tasks.append({
            "data": {
                "text": record_json
            },
            "annotations": [
                {
                    "result": [
                        {
                            "value": {"choices": [row["annotation_auto"]]},
                            "from_name": "label",
                            "to_name": "text",
                            "type": "choices"
                        }
                    ]
                }
            ]
        })

    resp = requests.post(
        f"{LABEL_STUDIO_URL}/api/projects/{project_id}/import",
        headers=get_headers({"Content-Type": "application/json"}),
        data=json.dumps(tasks)
    )
    resp.raise_for_status()

    print(f"📤 Importados {len(tasks)} registros para o Label Studio.")

    context["ti"].xcom_push(key="project_id", value=project_id)
    context["ti"].xcom_push(key="gold_path", value=gold_path)


# ============================================================
# EXPORTAR ANOTAÇÕES
# ============================================================

def fetch_annotations(**context):

    project_id = context["ti"].xcom_pull(key="project_id")

    resp = requests.get(
        f"{LABEL_STUDIO_URL}/api/projects/{project_id}/export?exportType=JSON",
        headers=get_headers()
    )
    resp.raise_for_status()
    data = resp.json()

    os.makedirs(LOCAL_ANNOT, exist_ok=True)
    annot_path = os.path.join(LOCAL_ANNOT, "annotations.json")

    with open(annot_path, "w") as f:
        json.dump(data, f, indent=2)

    context["ti"].xcom_push(key="annot_path", value=annot_path)

    print(f"📥 Exportado: {annot_path}")


# ============================================================
# GERAR GOLD FINAL
# ============================================================

def build_annotated_gold(**context):
    from core.minio_handler import ensure_bucket, upload_file

    gold_path = context["ti"].xcom_pull(key="gold_path")
    annot_path = context["ti"].xcom_pull(key="annot_path")

    df_gold = pd.read_parquet(gold_path)
    df_gold["gold_key"] = df_gold.index.astype(str)

    with open(annot_path) as f:
        raw = json.load(f)

    rows = []
    for item in raw:
        record_json = item["data"]["text"]
        original = json.loads(record_json)

        gold_key = original["gold_key"]  # agora sim!
        label = item["annotations"][0]["result"][0]["value"]["choices"][0]

        rows.append({
            "gold_key": gold_key,
            "annotation": label
        })

    df_anot = pd.DataFrame(rows)

    df_final = df_gold.merge(df_anot, on="gold_key", how="left")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_name = f"gold_annotated_{timestamp}.parquet"
    final_path = os.path.join(LOCAL_ANNOT, final_name)

    df_final.to_parquet(final_path, index=False)

    ensure_bucket(ANNOTATED_BUCKET)
    upload_file(final_path, ANNOTATED_BUCKET, final_name)

    print(f"✨ Gold anotado gerado: {final_name}")


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="annotations",
    start_date=datetime(2025, 11, 14),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["annotations", "label-studio", "gold"]
) as dag:

    import_task = PythonOperator(
        task_id="import_gold",
        python_callable=import_gold_to_labelstudio,
        provide_context=True
    )

    fetch_task = PythonOperator(
        task_id="fetch_annotations",
        python_callable=fetch_annotations,
        provide_context=True
    )

    build_task = PythonOperator(
        task_id="build_annotated_gold",
        python_callable=build_annotated_gold,
        provide_context=True
    )

    success_task = PythonOperator(
        task_id="log_success",
        python_callable=lambda: print("🎯 Pipeline concluído com sucesso!")
    )

    import_task >> fetch_task >> build_task >> success_task
