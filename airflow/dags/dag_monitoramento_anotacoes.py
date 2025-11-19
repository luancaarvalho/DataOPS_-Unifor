from __future__ import annotations

from datetime import datetime
import os

import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "owner": "dieb",
    "retries": 0,  # pra não ficar em up_for_retry
}


def get_engine():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_sqlalchemy_engine()


def run_sql_from_file(path: str):
    engine = get_engine()
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo SQL não encontrado em {path}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(sql)


with DAG(
    dag_id="monitoramento_anotacoes",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # só roda manual
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["anotacoes", "dataops"],
) as dag:

    # 1) Criação das tabelas base
    @task
    def create_tables():
        sql_path = "/opt/airflow/include/sql/create_tables.sql"
        run_sql_from_file(sql_path)

    # 2) Ingestão dos CSVs brutos
    @task
    def ingest_reviews():
        csv_path = "/opt/airflow/data/raw/reviews.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Arquivo {csv_path} não encontrado")

        df = pd.read_csv(csv_path)
        engine = get_engine()
        df.to_sql("raw_reviews", engine, if_exists="replace", index=False)

    @task
    def ingest_annotations():
        csv_path = "/opt/airflow/data/raw/annotations.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Arquivo {csv_path} não encontrado")

        df = pd.read_csv(csv_path)
        engine = get_engine()
        df.to_sql("raw_annotations", engine, if_exists="replace", index=False)

    # 3) Transformação: join reviews + anotações
    @task
    def transform() -> int:
        """
        Atualiza fact_review_annotations como join 1:1 entre raw_reviews e raw_annotations.
        Usa TRUNCATE + INSERT pra não derrubar a tabela (e nem a view que depende dela).
        """
        engine = get_engine()

        df = pd.read_sql(
            """
            SELECT
                r.review_id,
                r.product_id,
                r.user_id,
                r.review_text,
                r.score,
                r.review_date,
                a.sentimento,
                a.tema_principal,
                a.fonte_anotacao
            FROM raw_reviews r
            JOIN raw_annotations a
                ON r.review_id = a.review_id
            """,
            engine,
        )

        # limpa a tabela sem dar DROP (preserva view bi_anotacoes_negocio)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE fact_review_annotations"))

        df.to_sql(
            "fact_review_annotations",
            engine,
            if_exists="append",
            index=False,
        )

        return len(df)

    # 4) Data Quality + métricas
    @task
    def data_quality(rows_fact: int) -> int:
        """
        Aplica regras de qualidade e registra métricas em dq_results.
        Usa dq_results como base para monitoramento técnico.
        """
        engine = get_engine()

        fact = pd.read_sql("SELECT * FROM fact_review_annotations", engine)
        ann = pd.read_sql("SELECT * FROM raw_annotations", engine)
        reviews = pd.read_sql("SELECT * FROM raw_reviews", engine)

        total_linhas = len(fact)

        # review_id não nulo
        erros_review_id_nulo = int(fact["review_id"].isnull().sum())

        # domínio de sentimento
        valid_sentimentos = {"positivo", "neutro", "negativo"}
        erros_sentimento = int((~fact["sentimento"].isin(valid_sentimentos)).sum())

        # annotations sem review correspondente
        ids_reviews = set(reviews["review_id"].unique())
        ids_ann = set(ann["review_id"].unique())
        erros_inconsistencia = len(ids_ann - ids_reviews)

        metrics = {
            "data_execucao": datetime.utcnow(),
            "total_linhas": total_linhas,
            "erros_sentimento": erros_sentimento,
            "erros_review_id_nulo": erros_review_id_nulo,
            "erros_inconsistencia": erros_inconsistencia,
        }

        with engine.begin() as conn:
            # garante existência da tabela de DQ
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    data_execucao TIMESTAMP,
                    total_linhas INT,
                    erros_sentimento INT,
                    erros_review_id_nulo INT,
                    erros_inconsistencia INT
                );
            """))

            # insere linha em dq_results
            conn.execute(
                text("""
                    INSERT INTO dq_results (
                        data_execucao,
                        total_linhas,
                        erros_sentimento,
                        erros_review_id_nulo,
                        erros_inconsistencia
                    )
                    VALUES (
                        :data_execucao,
                        :total_linhas,
                        :erros_sentimento,
                        :erros_review_id_nulo,
                        :erros_inconsistencia
                    )
                """),
                metrics,
            )

        # aqui eu não quebro a DAG se houver erro de DQ, só registro
        return total_linhas

    # 5) BI – visão de negócio
    @task
    def build_bi_business():
        engine = get_engine()
        sql = """
        CREATE OR REPLACE VIEW bi_anotacoes_negocio AS
        SELECT
            review_date::date AS dia,
            product_id,
            COUNT(*) AS qtd_reviews,
            AVG(CASE WHEN sentimento = 'positivo' THEN 1 ELSE 0 END) AS pct_positivo,
            AVG(CASE WHEN sentimento = 'negativo' THEN 1 ELSE 0 END) AS pct_negativo
        FROM fact_review_annotations
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
        with engine.begin() as conn:
            conn.execute(sql)

    # 6) BI – monitoramento (só dependência)
    @task
    def build_bi_monitoring(_: int):
        return "ok"

    ct = create_tables()
    ir = ingest_reviews()
    ia = ingest_annotations()
    tf = transform()
    dq = data_quality(tf)
    bi_neg = build_bi_business()
    bi_mon = build_bi_monitoring(dq)

    ct >> [ir, ia] >> tf >> dq >> [bi_neg, bi_mon]