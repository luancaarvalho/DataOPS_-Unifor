from __future__ import annotations

from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import text
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "owner": "dieb",
    "retries": 0,
}

LIMITE_ALERTA = 5.35  # threshold para alert_flag


# ---------------------------------------------
# Conexão com Postgres
# ---------------------------------------------
def get_engine():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_sqlalchemy_engine()


# ---------------------------------------------
# Log genérico de eventos do pipeline
# ---------------------------------------------
def log_pipeline_event(task: str, status: str, msg: str | None = None, nova_linha_fato: int | None = None):
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
            VALUES (NOW(), NOW(), :task, :status, :msg, :nova_linha_fato)
        """), {
            "task": task,
            "status": status,
            "msg": msg,
            "nova_linha_fato": nova_linha_fato,
        })


# ---------------------------------------------
# Datasets (Data-Aware Scheduling)
# ---------------------------------------------
FX_MONITORAMENTO_DS = Dataset("postgres://fx_usdbrl_monitoramento")
FX_BI_SEMANAL_DS = Dataset("postgres://bi_fx_cambio_negocio")


# =====================================================================
# DAG 1 – PIPELINE PRINCIPAL (ingestão → anotação → DQ → BI semanal)
# =====================================================================
with DAG(
    dag_id="monitoramento_cambio_anotacoes",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["anotacoes", "dataops", "cambio"],
) as dag1:

    # ---------------------------------------------
    # 1. Criação das tabelas
    # ---------------------------------------------
    @task
    def create_tables():
        engine = get_engine()
        with engine.begin() as conn:

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fx_usdbrl_monitoramento (
                    id SERIAL PRIMARY KEY,
                    ref_date DATE NOT NULL UNIQUE,
                    ref_timestamp TIMESTAMP NOT NULL,
                    code TEXT,
                    codein TEXT,
                    name TEXT,
                    bid NUMERIC,
                    ask NUMERIC,
                    pct_change NUMERIC,
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

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fx_relatorios_semanais (
                    semana DATE PRIMARY KEY,
                    bid_medio_semana NUMERIC,
                    ask_medio_semana NUMERIC,
                    dias_alerta INT,
                    dias_com_dado INT,
                    pct_dias_alerta NUMERIC,
                    risco_semana INT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """))

    # ---------------------------------------------
    # 2. Ingestão – usando logical_date
    # ---------------------------------------------
    @task
    def get_exchange_rates(logical_date=None) -> dict:
        dia = logical_date.date() if logical_date else datetime.utcnow().date()

        start_end = dia.strftime("%Y%m%d")
        url = (
            "https://economia.awesomeapi.com.br/json/daily/USD-BRL/"
            f"?start_date={start_end}&end_date={start_end}"
        )

        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Caso sem dados (fim de semana / feriado / falha da API)
        if not data:
            return {
                "ref_date": dia.isoformat(),
                "code": "USD",
                "codein": "BRL",
                "name": "Dolar Comercial",
                "bid": None,
                "ask": None,
                "pct_change": None,
                "timestamp": int(datetime.combine(dia, datetime.min.time()).timestamp()),
            }

        usdbrl = data[0]

        return {
            "ref_date": dia.isoformat(),
            "code": usdbrl.get("code"),
            "codein": usdbrl.get("codein"),
            "name": usdbrl.get("name"),
            "bid": float(usdbrl.get("bid")),
            "ask": float(usdbrl.get("ask")),
            "pct_change": float(usdbrl.get("pctChange")),
            "timestamp": int(usdbrl.get("timestamp")),
        }

    # ---------------------------------------------
    # 3. Anotação (alert_flag)
    # ---------------------------------------------
    @task
    def annotate_rates(record: dict) -> dict:
        if record["bid"] is None:
            record["alert_flag"] = None
        else:
            record["alert_flag"] = 1 if record["bid"] > LIMITE_ALERTA else 0
        return record

    # ---------------------------------------------
    # 4. Validação + UPSERT (idempotente por dia)
    #    → emite Dataset FX_MONITORAMENTO_DS
    # ---------------------------------------------
    @task(outlets=[FX_MONITORAMENTO_DS])
    def validate_and_save(record: dict) -> int:
        try:
            # Caso sem dado (fim de semana/feriado): registra linha "nula"
            if record["bid"] is None or record["ask"] is None:
                engine = get_engine()
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO fx_usdbrl_monitoramento
                            (ref_date, ref_timestamp, code, codein, name,
                             bid, ask, pct_change, alert_flag)
                        VALUES
                            (:ref_date,
                             TO_TIMESTAMP(:timestamp),
                             :code, :codein, :name,
                             :bid, :ask, :pct_change, :alert_flag)
                        ON CONFLICT (ref_date)
                        DO UPDATE SET
                            ref_timestamp = EXCLUDED.ref_timestamp,
                            code         = EXCLUDED.code,
                            codein       = EXCLUDED.codein,
                            name         = EXCLUDED.name,
                            bid          = EXCLUDED.bid,
                            ask          = EXCLUDED.ask,
                            pct_change   = EXCLUDED.pct_change,
                            alert_flag   = EXCLUDED.alert_flag;
                    """), record)

                log_pipeline_event(
                    task="validate_and_save",
                    status="skipped",
                    msg="Sem dados de bid/ask para o dia",
                    nova_linha_fato=0,
                )
                return 0

            # Validações simples
            if record["bid"] <= 0:
                raise ValueError(f"Bid inválido: {record['bid']}")
            if record["ask"] <= 0:
                raise ValueError(f"Ask inválido: {record['ask']}")

            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO fx_usdbrl_monitoramento
                        (ref_date, ref_timestamp, code, codein, name,
                         bid, ask, pct_change, alert_flag)
                    VALUES
                        (:ref_date,
                         TO_TIMESTAMP(:timestamp),
                         :code, :codein, :name,
                         :bid, :ask, :pct_change, :alert_flag)
                    ON CONFLICT (ref_date)
                    DO UPDATE SET
                        ref_timestamp = EXCLUDED.ref_timestamp,
                        code         = EXCLUDED.code,
                        codein       = EXCLUDED.codein,
                        name         = EXCLUDED.name,
                        bid          = EXCLUDED.bid,
                        ask          = EXCLUDED.ask,
                        pct_change   = EXCLUDED.pct_change,
                        alert_flag   = EXCLUDED.alert_flag;
                """), record)

            log_pipeline_event(
                task="validate_and_save",
                status="success",
                msg=None,
                nova_linha_fato=1,
            )
            return 1

        except Exception as e:
            log_pipeline_event(
                task="validate_and_save",
                status="failed",
                msg=str(e),
                nova_linha_fato=0,
            )
            raise

    # ---------------------------------------------
    # 5. Data Quality
    # ---------------------------------------------
    @task
    def data_quality(_: int) -> int:
        engine = get_engine()
        df = pd.read_sql("SELECT * FROM fx_usdbrl_monitoramento", engine)

        total = len(df)

        cond_bid_err = df["bid"].notnull() & (df["bid"] <= 0)
        cond_ask_err = df["ask"].notnull() & (df["ask"] <= 0)

        erros_bid = int(cond_bid_err.sum())
        erros_ask = int(cond_ask_err.sum())

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
                    data_execucao,
                    total_registros,
                    erros_bid_nao_positivo,
                    erros_ask_nao_positivo,
                    nulos_bid,
                    nulos_ask
                )
                VALUES (
                    :data_execucao,
                    :total_registros,
                    :erros_bid_nao_positivo,
                    :erros_ask_nao_positivo,
                    :nulos_bid,
                    :nulos_ask
                )
            """), metrics)

        return total

    # ---------------------------------------------
    # 6. BI – visão de negócio por semana
    #    → emite Dataset FX_BI_SEMANAL_DS
    # ---------------------------------------------
    @task(outlets=[FX_BI_SEMANAL_DS])
    def build_bi():
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE OR REPLACE VIEW bi_fx_cambio_negocio AS
                SELECT
                    date_trunc('week', ref_date)::date AS semana,
                    AVG(bid) AS bid_medio_semana,
                    AVG(ask) AS ask_medio_semana,
                    SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS dias_alerta,
                    COUNT(*) AS dias_com_dado,
                    CASE
                        WHEN COUNT(*) = 0 THEN 0
                        ELSE SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END)::float
                             / COUNT(*)
                    END AS pct_dias_alerta
                FROM fx_usdbrl_monitoramento
                GROUP BY date_trunc('week', ref_date)
                ORDER BY semana;
            """))

    # ---------------------------------------------
    # 7. Monitoramento técnico agregado
    # ---------------------------------------------
    @task
    def monitor_pipeline(total_inserted: int):
        log_pipeline_event(
            task="pipeline_run",
            status="success",
            msg=None,
            nova_linha_fato=total_inserted,
        )

    # Encadeamento
    ct = create_tables()
    rec = get_exchange_rates()
    ann = annotate_rates(rec)
    saved = validate_and_save(ann)
    dq = data_quality(saved)
    bi = build_bi()
    mon = monitor_pipeline(saved)

    ct >> rec >> ann >> saved >> dq >> [bi, mon]


# =====================================================================
# DAG 2 – RELATÓRIOS SEMANAIS (Data-Driven + Dynamic Task Mapping)
# =====================================================================
with DAG(
    dag_id="fx_bi_relatorios_semanais",
    start_date=datetime(2025, 11, 1),
    schedule=[FX_BI_SEMANAL_DS],  # dispara quando o BI é atualizado
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bi", "dataops", "cambio"],
) as dag2:

    @task
    def get_semanas_unicas() -> list[str]:
        """
        Busca semanas disponíveis na view de BI.
        A view bi_fx_cambio_negocio já traz os campos agregados por semana.
        """
        engine = get_engine()
        df = pd.read_sql("SELECT DISTINCT semana FROM bi_fx_cambio_negocio", engine)
        return df["semana"].astype(str).tolist()

    @task
    def gerar_relatorio_semana(semana: str):
        """
        Gera um resumo por semana e grava em fx_relatorios_semanais.
        Aqui criamos a flag risco_semana (exclusiva desta DAG):
        risco_semana = 1 se pct_dias_alerta > 0.5, senão 0.
        """
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
                WHERE semana = CAST(:semana AS date)
            """), {"semana": semana}).fetchone()

            if not row:
                return

            # Flag de risco semanal: mais de 50% dos dias em alerta
            risco_semana = 1 if (row.pct_dias_alerta is not None and row.pct_dias_alerta > 0.5) else 0

            conn.execute(text("""
                INSERT INTO fx_relatorios_semanais (
                    semana,
                    bid_medio_semana,
                    ask_medio_semana,
                    dias_alerta,
                    dias_com_dado,
                    pct_dias_alerta,
                    risco_semana
                )
                VALUES (
                    :semana,
                    :bid_medio_semana,
                    :ask_medio_semana,
                    :dias_alerta,
                    :dias_com_dado,
                    :pct_dias_alerta,
                    :risco_semana
                )
                ON CONFLICT (semana) DO UPDATE SET
                    bid_medio_semana = EXCLUDED.bid_medio_semana,
                    ask_medio_semana = EXCLUDED.ask_medio_semana,
                    dias_alerta      = EXCLUDED.dias_alerta,
                    dias_com_dado    = EXCLUDED.dias_com_dado,
                    pct_dias_alerta  = EXCLUDED.pct_dias_alerta,
                    risco_semana     = EXCLUDED.risco_semana;
            """), {
                "semana": row.semana,
                "bid_medio_semana": row.bid_medio_semana,
                "ask_medio_semana": row.ask_medio_semana,
                "dias_alerta": row.dias_alerta,
                "dias_com_dado": row.dias_com_dado,
                "pct_dias_alerta": row.pct_dias_alerta,
                "risco_semana": risco_semana,
            })

    semanas = get_semanas_unicas()
    gerar_relatorio_semana.expand(semana=semanas)