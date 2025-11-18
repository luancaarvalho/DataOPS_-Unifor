# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from include.expectations.gold_expectations import GoldDataset


GOLD_BUCKET = "gold"
LOCAL_GOLD = "/opt/airflow/data/gold"


def classify_auto(row):
    duracao = row.get("duracao_contato")
    num_cont = row.get("num_contatos_campanha")
    faixa_idade = row.get("faixa_idade")
    resultado = row.get("resultado_ultimo_contato")
    dias_ultimo = row.get("dias_desde_ultimo_contato")
    intensidade = row.get("intensidade_contato")
    alvo = row.get("alvo")
    eficiencia = row.get("eficiencia_campanha")
    eh_rec = row.get("eh_recorrente")

    if (
        duracao and duracao > 180 and
        intensidade in ["single", "low"] and
        faixa_idade in ["25-34", "35-44"] and
        eficiencia and eficiencia > 0.4
    ):
        return "altamente_interessado"

    if (
        duracao and duracao > 90 and
        intensidade != "high" and
        (resultado == "success" or eh_rec)
    ):
        return "interessado"

    if (
        40 <= (duracao or 0) <= 120 and
        num_cont <= 3 and
        intensidade in ["single", "low", "medium"]
    ):
        return "neutro"

    if (
        (duracao and duracao < 40) or
        resultado == "failure" or
        num_cont > 5 or
        dias_ultimo == 999
    ):
        return "desinteressado"

    if alvo == 0:
        return "rejeitou"

    return "neutro"


def build_gold(**context):
    from core.minio_handler import ensure_bucket, upload_file

    os.makedirs(LOCAL_GOLD, exist_ok=True)

    silver_path = context["dag_run"].conf.get("silver_file_path")
    if not silver_path or not os.path.exists(silver_path):
        raise FileNotFoundError(f"Arquivo Silver não encontrado: {silver_path}")

    print(f"📄 Usando arquivo Silver local: {silver_path}")

    df = pd.read_parquet(silver_path)

    # 🔹 Transformações e criação das colunas esperadas pelo Data Quality
    numeric_cols = [
        "age", "duration", "campaign", "pdays", "previous",
        "emp_var_rate", "cons_price_idx", "cons_conf_idx",
        "euribor3m", "nr_employed",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    binary_map = {"yes": 1, "no": 0}
    for col in ["default", "housing", "loan", "y"]:
        if col in df.columns:
            df[col] = df[col].map(binary_map).fillna(0).astype(int)

    def age_group(age):
        if pd.isna(age):
            return "unknown"
        age = int(age)
        if age < 25: return "18-24"
        elif age < 35: return "25-34"
        elif age < 45: return "35-44"
        elif age < 60: return "45-59"
        return "60+"

    df["age_group"] = df["age"].apply(age_group)

    df["call_duration_group"] = pd.cut(
        df["duration"],
        bins=[0, 60, 180, 600, 1800, 999999],
        labels=["0-1min", "1-3min", "3-10min", "10-30min", "30+min"],
        include_lowest=True
    )

    df["is_recorrent"] = df["previous"] > 0

    df["contact_intensity"] = pd.cut(
        df["campaign"],
        bins=[0, 1, 3, 10, 9999],
        labels=["single", "low", "medium", "high"],
        include_lowest=True
    )

    df["campaign_efficiency"] = df["y"] / (df["campaign"] + 1)

    rename_map = {
        "age": "idade",
        "job": "profissao",
        "marital": "estado_civil",
        "education": "escolaridade",
        "default": "inadimplente",
        "housing": "financiamento_moradia",
        "loan": "emprestimo_pessoal",
        "contact": "meio_contato",
        "month": "mes_contato",
        "day_of_week": "dia_semana",
        "duration": "duracao_contato",
        "campaign": "num_contatos_campanha",
        "pdays": "dias_desde_ultimo_contato",
        "previous": "num_contatos_anteriores",
        "poutcome": "resultado_ultimo_contato",
        "emp_var_rate": "variacao_emprego",
        "cons_price_idx": "indice_precos_consumidor",
        "cons_conf_idx": "indice_confianca_consumidor",
        "euribor3m": "euribor_3m",
        "nr_employed": "num_empregados",
        "y": "alvo",
        "education_level": "nivel_escolaridade",
        "age_group": "faixa_idade",
        "call_duration_group": "faixa_duracao_contato",
        "is_recorrent": "eh_recorrente",
        "contact_intensity": "intensidade_contato",
        "campaign_efficiency": "eficiencia_campanha",
    }
    df.rename(columns=rename_map, inplace=True)

    # 🔹 Agora que as colunas existem → Pode validar!
    dq = GoldDataset.validate(df)

    if not dq["success"]:
        print("❌ Falhas de Data Quality:")
        for fail in dq["failed_expectations"]:
            print(f" - {fail}")
        raise ValueError("Data Quality falhou! Corrija antes de prosseguir ❌")

    print("✅ Data Quality: OK!")

    # 🔹 Criar anotação automática
    df["annotation_auto"] = df.apply(classify_auto, axis=1)

    # 🔹 Salvar Gold final
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gold_name = f"gold_dataset_{timestamp}.parquet"
    gold_path = os.path.join(LOCAL_GOLD, gold_name)

    df.to_parquet(gold_path, index=False)
    print(f"💾 Gold salvo: {gold_path}")

    ensure_bucket(GOLD_BUCKET)
    upload_file(gold_path, GOLD_BUCKET, gold_name)

    print(f"✨ Enviado ao MinIO: {gold_name}")


def log_success():
    print("Gold concluído com sucesso! 🚀")


with DAG(
    dag_id="gold",
    start_date=datetime(2025, 11, 14),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["gold"],
) as dag:

    build_task = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
        provide_context=True
    )

    success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success
    )

    build_task >> success_task
