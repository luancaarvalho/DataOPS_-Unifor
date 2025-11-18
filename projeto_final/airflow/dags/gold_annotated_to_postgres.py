# -*- coding: utf-8 -*-
import os
import hashlib
import numpy as np
import pandas as pd

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import psycopg2
import psycopg2.extras


LOCAL_ANNOT = "/opt/airflow/data/annotated"


def get_latest_annotated():
    files = [f for f in os.listdir(LOCAL_ANNOT) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo annotated encontrado.")
    files.sort(reverse=True)
    return os.path.join(LOCAL_ANNOT, files[0])


def get_pg_conn():
    conn = BaseHook.get_connection("postgres_airflow")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        database=conn.schema,
        user=conn.login,
        password=conn.password
    )


def clean_annotation(value):
    """Garante que o valor nunca será NULL."""
    if value in (None, np.nan, "", "null", "None"):
        return "neutro"

    if isinstance(value, (list, dict)):
        return "neutro"

    return str(value)


def stable_hash(row):
    """
    Gera um hash estável, seguro para BIGINT (range signed 64-bit).
    """
    h = hashlib.sha256(str(tuple(row)).encode()).hexdigest()
    val = int(h[:16], 16)
    return val & 0x7FFFFFFFFFFFFFFF  # garante BIGINT positivo (signed 64-bit)


def load_gold_annotated_to_postgres(**context):
    parquet_path = get_latest_annotated()
    print(f"📂 Lendo parquet: {parquet_path}")

    # ---------------------------------------------------------
    # LER PARQUET
    # ---------------------------------------------------------
    df = pd.read_parquet(parquet_path)

    # converter categorias para string
    for col in df.select_dtypes(["category"]).columns:
        df[col] = df[col].astype(str)

    df["annotation"] = df["annotation"].apply(clean_annotation)
    df["annotation"] = df["annotation"].astype(str).fillna("neutro")

    # hashing estável
    df_hashable = df.fillna("").astype(str)
    df["row_id"] = df_hashable.apply(stable_hash, axis=1)

    # ---------------------------------------------------------
    # CONEXÃO COM POSTGRES
    # ---------------------------------------------------------
    conn = get_pg_conn()
    cur = conn.cursor()

    create_sql = """
        CREATE TABLE IF NOT EXISTS gold_annotated (
            row_id BIGINT PRIMARY KEY,
            idade INT,
            profissao TEXT,
            estado_civil TEXT,
            escolaridade TEXT,
            inadimplente INT,
            financiamento_moradia INT,
            emprestimo_pessoal INT,
            meio_contato TEXT,
            mes_contato TEXT,
            dia_semana TEXT,
            duracao_contato INT,
            num_contatos_campanha INT,
            dias_desde_ultimo_contato INT,
            num_contatos_anteriores INT,
            resultado_ultimo_contato TEXT,
            variacao_emprego FLOAT,
            indice_precos_consumidor FLOAT,
            indice_confianca_consumidor FLOAT,
            euribor_3m FLOAT,
            num_empregados FLOAT,
            alvo INT,
            nivel_escolaridade TEXT,
            faixa_idade TEXT,
            faixa_duracao_contato TEXT,
            eh_recorrente BOOLEAN,
            intensidade_contato TEXT,
            eficiencia_campanha FLOAT,
            annotation TEXT NOT NULL
        );
    """
    cur.execute(create_sql)
    conn.commit()

    # ---------------------------------------------------------
    # EVITA DUPLICATAS NO BANCO
    # ---------------------------------------------------------
    cur.execute("SELECT row_id FROM gold_annotated")
    existing_ids = {r[0] for r in cur.fetchall()}

    df_new = df[~df["row_id"].isin(existing_ids)]

    print(f"📌 Registros já existentes: {len(existing_ids)}")
    print(f"🆕 Novos registros a inserir: {len(df_new)}")

    if df_new.empty:
        print("✔ Nada novo para inserir.")
        return

    # ---------------------------------------------------------
    # DEBUG DOS VALORES
    # ---------------------------------------------------------
    print("\n===== DEBUG: MAX/MIN VALUES =====")
    for col in df_new.columns:
        if df_new[col].dtype in ["int64", "float64"]:
            print(f"{col}: min={df_new[col].min()}, max={df_new[col].max()}")
    print("=================================\n")

    # ---------------------------------------------------------
    # SANITIZAÇÃO CONTRA NumericValueOutOfRange
    # ---------------------------------------------------------
    int_cols = [
        "idade",
        "inadimplente",
        "financiamento_moradia",
        "emprestimo_pessoal",
        "duracao_contato",
        "num_contatos_campanha",
        "dias_desde_ultimo_contato",
        "num_contatos_anteriores",
        "alvo",
    ]

    for col in int_cols:
        if col in df_new.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
            df_new[col] = df_new[col].fillna(0)
            df_new[col] = df_new[col].clip(-2_147_483_648, 2_147_483_647)

    # ---------------------------------------------------------
    # INSERÇÃO FINAL
    # ---------------------------------------------------------

    ordered_cols = [
        "row_id",
        "idade",
        "profissao",
        "estado_civil",
        "escolaridade",
        "inadimplente",
        "financiamento_moradia",
        "emprestimo_pessoal",
        "meio_contato",
        "mes_contato",
        "dia_semana",
        "duracao_contato",
        "num_contatos_campanha",
        "dias_desde_ultimo_contato",
        "num_contatos_anteriores",
        "resultado_ultimo_contato",
        "variacao_emprego",
        "indice_precos_consumidor",
        "indice_confianca_consumidor",
        "euribor_3m",
        "num_empregados",
        "alvo",
        "nivel_escolaridade",
        "faixa_idade",
        "faixa_duracao_contato",
        "eh_recorrente",
        "intensidade_contato",
        "eficiencia_campanha",
        "annotation"
    ]

    insert_sql = f"""
        INSERT INTO gold_annotated (
            {", ".join(ordered_cols)}
        ) VALUES %s
        ON CONFLICT (row_id) DO NOTHING;
    """

    records = [
        tuple(row[col] for col in ordered_cols)
        for _, row in df_new.iterrows()
    ]

    psycopg2.extras.execute_values(cur, insert_sql, records, page_size=500)

    conn.commit()
    cur.close()
    conn.close()

    print("🚀 Inserção finalizada SEM NULOS! 🎉")


def log_success():
    print("🎯 gold_annotated_to_postgres finalizada com sucesso!")


with DAG(
    dag_id="gold_annotated_to_postgres",
    start_date=datetime(2025, 11, 16),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["gold", "annotated", "superset"],
) as dag:

    load_task = PythonOperator(
        task_id="load_gold_annotated",
        python_callable=load_gold_annotated_to_postgres,
    )

    success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    load_task >> success_task
