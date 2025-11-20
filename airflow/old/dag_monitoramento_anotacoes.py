from __future__ import annotations

# =====================================================================
# IMPORTS
# =====================================================================
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import text
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset


# =====================================================================
# CONFIG
# =====================================================================
DEFAULT_ARGS = {"owner": "dieb", "retries": 0}
LIMITE_ALERTA = 5.35

# Dataset usado como trigger da DAG 2
FX_BI_SEMANAL_DS = Dataset("postgresql://metabase/bi_fx_cambio_negocio")


# =====================================================================
# HELPERS
# =====================================================================
def get_engine():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_sqlalchemy_engine()


def log_pipeline_event(task, status, msg=None, nova_linha_fato=None):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bi_fx_monitoramento_pipeline (
                data_execucao,
                execution_date,
                task,
                status,
                mensagem_erro,
                nova_linha_fato
            )
            VALUES (NOW(), NOW(), :task, :status, :msg, :nova)
        """), {
            "task": task,
            "status": status,
            "msg": msg,
            "nova": nova_linha_fato
        })


def task_with_logging(task_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                log_pipeline_event(task_name, "success")
                return result
            except Exception as e:
                log_pipeline_event(task_name, "failed", msg=str(e))
                raise
        return wrapper
    return decorator


# =====================================================================
# DAG 1 — INGESTÃO + ANOTAÇÃO + BI
# =====================================================================
with DAG(
    dag_id="monitoramento_cambio_anotacoes",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["dataops", "cambio", "anotacoes"],
) as dag:

    # ----------------------------------------------------------
    @task
    @task_with_logging("create_tables")
    def create_tables():
        engine = get_engine()
        with engine.begin() as conn:

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fx_usdbrl_monitoramento (
                    id SERIAL PRIMARY KEY,
                    ref_date DATE NOT NULL UNIQUE,
                    ref_timestamp TIMESTAMP NOT NULL,
                    code TEXT, codein TEXT, name TEXT,
                    bid NUMERIC, ask NUMERIC, pct_change NUMERIC,
                    alert_flag INT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fx_dq_results (
                    data_execucao TIMESTAMP,
                    total_registros INT,
                    erros_bid_nao_positivo INT,
                    erros_ask_nao_positivo INT,
                    nulos_bid INT,
                    nulos_ask INT
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bi_fx_monitoramento_pipeline (
                    data_execucao TIMESTAMP,
                    execution_date TIMESTAMP,
                    task TEXT,
                    status TEXT,
                    mensagem_erro TEXT,
                    nova_linha_fato INT
                );
            """))

    # ----------------------------------------------------------
    @task
    @task_with_logging("get_exchange_rates")
    def get_exchange_rates(logical_date=None):

        dia = logical_date.date() if logical_date else datetime.utcnow().date()
        start_end = dia.strftime("%Y%m%d")

        url = f"https://economia.awesomeapi.com.br/json/daily/USD-BRL/?start_date={start_end}&end_date={start_end}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            ts = int(datetime.combine(dia, datetime.min.time()).timestamp())
            return {
                "ref_date": dia.isoformat(),
                "code": "USD", "codein": "BRL", "name": "Dolar Comercial",
                "bid": None, "ask": None, "pct_change": None,
                "timestamp": ts,
            }

        usd = data[0]

        return {
            "ref_date": dia.isoformat(),
            "code": usd.get("code"),
            "codein": usd.get("codein"),
            "name": usd.get("name"),
            "bid": float(usd.get("bid")),
            "ask": float(usd.get("ask")),
            "pct_change": float(usd.get("pctChange")),
            "timestamp": int(usd.get("timestamp")),
        }

    # ----------------------------------------------------------
    @task
    @task_with_logging("annotate_rates")
    def annotate_rates(record: dict) -> dict:
        if record["bid"] is None:
            record["alert_flag"] = None
        else:
            record["alert_flag"] = 1 if record["bid"] > LIMITE_ALERTA else 0
        return record

    # ----------------------------------------------------------
    @task
    @task_with_logging("validate_and_save")
    def validate_and_save(record: dict) -> int:

        if record["bid"] is not None and record["bid"] <= 0:
            raise ValueError("Bid inválido")

        if record["ask"] is not None and record["ask"] <= 0:
            raise ValueError("Ask inválido")

        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fx_usdbrl_monitoramento (
                    ref_date, ref_timestamp, code, codein, name,
                    bid, ask, pct_change, alert_flag
                )
                VALUES (
                    DATE(TO_TIMESTAMP(:timestamp)),
                    TO_TIMESTAMP(:timestamp),
                    :code, :codein, :name,
                    :bid, :ask, :pct_change, :alert_flag
                )
                ON CONFLICT (ref_date) DO UPDATE SET
                    bid = EXCLUDED.bid,
                    ask = EXCLUDED.ask,
                    pct_change = EXCLUDED.pct_change,
                    alert_flag = EXCLUDED.alert_flag,
                    ref_timestamp = EXCLUDED.ref_timestamp;
            """), record)

        return 1

    # ----------------------------------------------------------
    @task
    @task_with_logging("data_quality")
    def data_quality(_: int) -> int:

        engine = get_engine()
        df = pd.read_sql("SELECT * FROM fx_usdbrl_monitoramento", engine)

        total = len(df)
        erros_bid = int((df["bid"].notnull() & (df["bid"] <= 0)).sum())
        erros_ask = int((df["ask"].notnull() & (df["ask"] <= 0)).sum())
        nulos_bid = int(df["bid"].isnull().sum())
        nulos_ask = int(df["ask"].isnull().sum())

        metrics = {
            "data_execucao": datetime.utcnow(),
            "total_registros": total,
            "erros_bid_nao_positivo": erros_bid,
            "erros_ask_nao_positivo": erros_ask,
            "nulos_bid": nulos_bid,
            "nulos_ask": nulos_ask,
        }

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO fx_dq_results (
                    data_execucao, total_registros,
                    erros_bid_nao_positivo, erros_ask_nao_positivo,
                    nulos_bid, nulos_ask
                )
                VALUES (
                    :data_execucao, :total_registros,
                    :erros_bid_nao_positivo, :erros_ask_nao_positivo,
                    :nulos_bid, :nulos_ask
                )
            """), metrics)

        return total

    # ----------------------------------------------------------
    @task(outlets=[FX_BI_SEMANAL_DS])
    @task_with_logging("build_bi")
    def build_bi():

        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE OR REPLACE VIEW bi_fx_cambio_negocio AS
                SELECT
                    ref_date AS dia,
                    DATE_TRUNC('week', ref_date) AS semana,
                    AVG(bid) AS bid_medio_semana,
                    AVG(ask) AS ask_medio_semana,
                    SUM(alert_flag) AS dias_alerta,
                    COUNT(*) FILTER (WHERE bid IS NOT NULL) AS dias_com_dado,
                    SUM(alert_flag)::float / NULLIF(COUNT(*), 0) AS pct_dias_alerta
                FROM fx_usdbrl_monitoramento
                GROUP BY 1,2
                ORDER BY 1;
            """))

    # ----------------------------------------------------------
    ct = create_tables()
    rec = get_exchange_rates()
    ann = annotate_rates(rec)
    saved = validate_and_save(ann)
    dq = data_quality(saved)
    bi = build_bi()

    ct >> rec >> ann >> saved >> dq >> bi


# =====================================================================
# DAG 2 — RELATÓRIOS SEMANAIS (DATA DRIVEN)
# =====================================================================
with DAG(
    dag_id="fx_bi_relatorios_semanais",
    start_date=datetime(2025, 11, 1),
    schedule=[FX_BI_SEMANAL_DS],
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bi", "semanais", "dataops"],
) as dag2:

    @task
    @task_with_logging("create_table_fx_relatorios_semanais")
    def create_relatorios_table():

        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fx_relatorios_semanais (
                    semana DATE PRIMARY KEY,
                    bid_medio_semana NUMERIC,
                    ask_medio_semana NUMERIC,
                    dias_alerta INT,
                    dias_com_dado INT,
                    pct_dias_alerta NUMERIC,
                    flag_semana_risco INT
                );
            """))

    @task
    @task_with_logging("get_semanas_unicas")
    def get_semanas_unicas() -> list[str]:

        engine = get_engine()
        df = pd.read_sql("SELECT DISTINCT semana FROM bi_fx_cambio_negocio", engine)
        return df["semana"].astype(str).tolist()

    @task
    @task_with_logging("gerar_relatorio_semana")
    def gerar_relatorio_semana(semana: str):

        engine = get_engine()

        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT
                    semana,
                    bid_medio_semana,
                    ask_medio_semana,
                    dias_alerta,
                    dias_com_dado,
                    pct_dias_alerta
                FROM bi_fx_cambio_negocio
                WHERE semana = :semana::date
            """), {"semana": semana}).fetchone()

            if not row:
                return

            flag_risco = 1 if row.pct_dias_alerta > 0.5 else 0

            conn.execute(text("""
                INSERT INTO fx_relatorios_semanais (
                    semana,
                    bid_medio_semana,
                    ask_medio_semana,
                    dias_alerta,
                    dias_com_dado,
                    pct_dias_alerta,
                    flag_semana_risco
                )
                VALUES (
                    :semana,
                    :bid,
                    :ask,
                    :dias_alerta,
                    :dias_dado,
                    :pct,
                    :flag
                )
                ON CONFLICT (semana) DO UPDATE SET
                    bid_medio_semana = EXCLUDED.bid_medio_semana,
                    ask_medio_semana = EXCLUDED.ask_medio_semana,
                    dias_alerta = EXCLUDED.dias_alerta,
                    dias_com_dado = EXCLUDED.dias_com_dado,
                    pct_dias_alerta = EXCLUDED.pct_dias_alerta,
                    flag_semana_risco = EXCLUDED.flag_semana_risco;
            """), {
                "semana": row.semana,
                "bid": row.bid_medio_semana,
                "ask": row.ask_medio_semana,
                "dias_alerta": row.dias_alerta,
                "dias_dado": row.dias_com_dado,
                "pct": row.pct_dias_alerta,
                "flag": flag_risco
            })

    create_tbl = create_relatorios_table()
    semanas = get_semanas_unicas()

    create_tbl >> gerar_relatorio_semana.expand(semana=semanas)