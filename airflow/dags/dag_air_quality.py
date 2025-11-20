from __future__ import annotations

from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import text
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "owner": "fabio",
    "retries": 0,
}

LIMITE_PM25 = 25  # limite diário recomendado pela OMS (não usado direto no AQI aqui, mas pode usar em regras futuras)
LIMITE_AQI = 80   # limite de alerta de risco respiratório


# -------------------------------------------------------------------
# Conexão com Postgres
# -------------------------------------------------------------------
def get_engine():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_sqlalchemy_engine()


# -------------------------------------------------------------------
# Logging genérico em air_pipeline_log
# -------------------------------------------------------------------
def log_pipeline_event(task: str, status: str, msg: str | None = None):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            INSERT INTO air_pipeline_log (
                data_execucao,
                task,
                status,
                mensagem
            )
            VALUES (NOW(), :task, :status, :msg)
            """
            ),
            {
                "task": task,
                "status": status,
                "msg": msg,
            },
        )


# -------------------------------------------------------------------
# Datasets (Data-Aware Scheduling)
# -------------------------------------------------------------------
AIR_DS = Dataset("postgres://air_quality_daily")
AIR_BI_DS = Dataset("postgres://air_quality_bi")


# ===============================================================
# DAG 1 – coleta → anotação → DQ → BI (diário)
# ===============================================================
with DAG(
    dag_id="monitoramento_qualidade_ar",
    start_date=datetime(2025, 11, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["air", "env", "dataops"],
):

    @task
    def create_tables():
        """
        Cria (se não existirem) as tabelas de fato diário,
        DQ, log técnico e BI semanal.
        """
        try:
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS air_quality_daily (
                        id SERIAL PRIMARY KEY,
                        ref_date DATE NOT NULL UNIQUE,
                        city TEXT,
                        aqi NUMERIC,
                        pm25 NUMERIC,
                        pm10 NUMERIC,
                        co NUMERIC,
                        risk_flag INT,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                    """
                    )
                )

                conn.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS air_dq_results (
                        data_execucao TIMESTAMP,
                        total_registros INT,
                        erros_pm25_neg INT,
                        erros_pm10_neg INT,
                        erros_aqi_neg INT,
                        nulos_pm25 INT,
                        nulos_pm10 INT,
                        nulos_aqi INT
                    );
                    """
                    )
                )

                conn.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS air_pipeline_log (
                        data_execucao TIMESTAMP,
                        task TEXT,
                        status TEXT,
                        mensagem TEXT
                    );
                    """
                    )
                )

                conn.execute(
                    text(
                        """
                    CREATE TABLE IF NOT EXISTS air_quality_bi (
                        semana DATE PRIMARY KEY,
                        pm25_media NUMERIC,
                        pm10_media NUMERIC,
                        dias_risco INT,
                        dias_validos INT,
                        pct_risco NUMERIC,
                        risco_ambiental INT,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                    """
                    )
                )

            log_pipeline_event("create_tables", "success", None)
        except Exception as e:
            log_pipeline_event("create_tables", "failed", str(e))
            raise

    @task
    def fetch_air_quality(logical_date=None) -> dict:
        """
        Busca dados horários de qualidade do ar (PM10, PM2.5, CO)
        na API Open-Meteo para o DIA da logical_date.
        """
        dia = logical_date.date() if logical_date else datetime.utcnow().date()

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": -23.55,  # São Paulo
            "longitude": -46.63,
            "hourly": "pm10,pm2_5,carbon_monoxide",
            "timezone": "America/Sao_Paulo",
            "start_date": dia.isoformat(),
            "end_date": dia.isoformat(),
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            msg = f"Erro ao chamar Open-Meteo para {dia}: {e}"
            print(f"[fetch_air_quality] {msg}")
            log_pipeline_event("fetch_air_quality", "failed", msg)
            return {
                "ref_date": dia.isoformat(),
                "city": "São Paulo",
                "aqi": None,
                "pm25": None,
                "pm10": None,
                "co": None,
            }

        hourly = data.get("hourly")
        if not hourly:
            msg = f"Resposta sem 'hourly' para {dia}: {data}"
            print(f"[fetch_air_quality] {msg}")
            log_pipeline_event("fetch_air_quality", "failed", msg)
            return {
                "ref_date": dia.isoformat(),
                "city": "São Paulo",
                "aqi": None,
                "pm25": None,
                "pm10": None,
                "co": None,
            }

        df = pd.DataFrame(hourly)
        if "time" not in df.columns:
            msg = f"'time' não encontrado em hourly: colunas={list(df.columns)}"
            print(f"[fetch_air_quality] {msg}")
            log_pipeline_event("fetch_air_quality", "failed", msg)
            return {
                "ref_date": dia.isoformat(),
                "city": "São Paulo",
                "aqi": None,
                "pm25": None,
                "pm10": None,
                "co": None,
            }

        df["time"] = pd.to_datetime(df["time"])
        df_dia = df[df["time"].dt.date == dia]

        if df_dia.empty:
            msg = f"Nenhum registro horário para {dia}"
            print(f"[fetch_air_quality] {msg}")
            log_pipeline_event("fetch_air_quality", "failed", msg)
            return {
                "ref_date": dia.isoformat(),
                "city": "São Paulo",
                "aqi": None,
                "pm25": None,
                "pm10": None,
                "co": None,
            }

        # Média diária
        pm25 = float(df_dia["pm2_5"].mean()) if "pm2_5" in df_dia.columns else None
        pm10 = float(df_dia["pm10"].mean()) if "pm10" in df_dia.columns else None
        co = (
            float(df_dia["carbon_monoxide"].mean())
            if "carbon_monoxide" in df_dia.columns
            else None
        )

        pm25_val = pm25 if pm25 is not None else 0.0
        pm10_val = pm10 if pm10 is not None else 0.0
        aqi = pm25_val * 2 + pm10_val

        record = {
            "ref_date": dia.isoformat(),
            "city": "São Paulo",
            "aqi": aqi if aqi > 0 else None,
            "pm25": pm25,
            "pm10": pm10,
            "co": co,
        }

        log_pipeline_event(
            "fetch_air_quality",
            "success",
            f"Dados coletados para {dia}: pm25={pm25}, pm10={pm10}, co={co}, aqi={record['aqi']}",
        )

        return record

    @task
    def annotate(record: dict) -> dict:
        """
        Cria a flag de risco respiratório diária com base no AQI.
        """
        if record["aqi"] is None:
            record["risk_flag"] = None
        else:
            record["risk_flag"] = 1 if record["aqi"] > LIMITE_AQI else 0

        log_pipeline_event(
            "annotate",
            "success",
            f"ref_date={record.get('ref_date')}, risk_flag={record.get('risk_flag')}",
        )
        return record

    @task(outlets=[AIR_DS])
    def save(record: dict):
        """
        UPSERT diário em air_quality_daily (idempotente por ref_date).
        """
        try:
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                    INSERT INTO air_quality_daily
                        (ref_date, city, aqi, pm25, pm10, co, risk_flag)
                    VALUES
                        (:ref_date, :city, :aqi, :pm25, :pm10, :co, :risk_flag)
                    ON CONFLICT (ref_date)
                    DO UPDATE SET
                        city      = EXCLUDED.city,
                        aqi       = EXCLUDED.aqi,
                        pm25      = EXCLUDED.pm25,
                        pm10      = EXCLUDED.pm10,
                        co        = EXCLUDED.co,
                        risk_flag = EXCLUDED.risk_flag;
                    """
                    ),
                    record,
                )

            log_pipeline_event(
                "save", "success", f"UPSERT em air_quality_daily para {record['ref_date']}"
            )
            return 1
        except Exception as e:
            log_pipeline_event(
                "save", "failed", f"Erro ao salvar ref_date={record.get('ref_date')}: {e}"
            )
            raise

    @task
    def dq(_: int):
        """
        Data Quality em air_quality_daily.
        - valores negativos
        - nulos
        """
        try:
            engine = get_engine()
            df = pd.read_sql("SELECT * FROM air_quality_daily", engine)

            metrics = {
                "data_execucao": datetime.utcnow(),
                "total_registros": len(df),
                "erros_pm25_neg": int((df["pm25"] < 0).sum()),
                "erros_pm10_neg": int((df["pm10"] < 0).sum()),
                "erros_aqi_neg": int((df["aqi"] < 0).sum()),
                "nulos_pm25": int(df["pm25"].isnull().sum()),
                "nulos_pm10": int(df["pm10"].isnull().sum()),
                "nulos_aqi": int(df["aqi"].isnull().sum()),
            }

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                    INSERT INTO air_dq_results VALUES (
                        :data_execucao, :total_registros,
                        :erros_pm25_neg, :erros_pm10_neg, :erros_aqi_neg,
                        :nulos_pm25, :nulos_pm10, :nulos_aqi
                    );
                    """
                    ),
                    metrics,
                )

            log_pipeline_event(
                "dq",
                "success",
                f"DQ executado: total={metrics['total_registros']} registros",
            )
            return 1
        except Exception as e:
            log_pipeline_event("dq", "failed", str(e))
            raise

    @task(outlets=[AIR_BI_DS])
    def build_bi():
        """
        View semanal agregando PM2.5, PM10 e risco.
        """
        try:
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                    CREATE OR REPLACE VIEW air_bi_view AS
                    SELECT
                        date_trunc('week', ref_date)::date AS semana,
                        AVG(pm25) AS pm25_media,
                        AVG(pm10) AS pm10_media,
                        SUM(CASE WHEN risk_flag = 1 THEN 1 ELSE 0 END) AS dias_risco,
                        COUNT(*) AS dias_validos,
                        SUM(CASE WHEN risk_flag = 1 THEN 1 ELSE 0 END)::float
                            / NULLIF(COUNT(*), 0) AS pct_risco
                    FROM air_quality_daily
                    GROUP BY 1
                    ORDER BY semana;
                    """
                    )
                )

            log_pipeline_event("build_bi", "success", "View air_bi_view atualizada")
        except Exception as e:
            log_pipeline_event("build_bi", "failed", str(e))
            raise

    # Encadeamento da DAG 1
    ct = create_tables()
    raw = fetch_air_quality()
    ann = annotate(raw)
    saved = save(ann)
    dq_run = dq(saved)
    bi = build_bi()

    ct >> raw >> ann >> saved >> dq_run >> bi


# ===============================================================
# DAG 2 – Relatórios Semanais (data-driven pelo Dataset AIR_BI_DS)
# ===============================================================
with DAG(
    dag_id="air_quality_relatorios_semanais",
    start_date=datetime(2025, 11, 1),
    schedule=[AIR_BI_DS],  # dispara quando o BI diário é atualizado
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["air", "bi", "dataops"],
):

    @task
    def get_semanas() -> list[str]:
        """
        Busca semanas distintas na view air_bi_view.
        """
        try:
            engine = get_engine()
            df = pd.read_sql("SELECT DISTINCT semana FROM air_bi_view", engine)
            semanas = df["semana"].astype(str).tolist()
            log_pipeline_event(
                "get_semanas",
                "success",
                f"{len(semanas)} semanas retornadas de air_bi_view",
            )
            return semanas
        except Exception as e:
            log_pipeline_event("get_semanas", "failed", str(e))
            raise

    @task
    def gerar_relatorio(semana: str):
        """
        Gera / atualiza o resumo semanal em air_quality_bi.
        risco_ambiental = 1 se pct_risco > 0.4, senão 0.
        """
        try:
            engine = get_engine()
            with engine.begin() as conn:
                row = conn.execute(
                    text(
                        """
                    SELECT
                        semana,
                        pm25_media,
                        pm10_media,
                        dias_risco,
                        dias_validos,
                        pct_risco
                    FROM air_bi_view
                    WHERE semana = CAST(:semana AS date)
                    """
                    ),
                    {"semana": semana},
                ).fetchone()

                if not row:
                    log_pipeline_event(
                        "gerar_relatorio",
                        "skipped",
                        f"Nenhum registro em air_bi_view para semana={semana}",
                    )
                    return

                risco = (
                    1
                    if (row.pct_risco is not None and row.pct_risco > 0.4)
                    else 0
                )

                conn.execute(
                    text(
                        """
                    INSERT INTO air_quality_bi (
                        semana,
                        pm25_media,
                        pm10_media,
                        dias_risco,
                        dias_validos,
                        pct_risco,
                        risco_ambiental
                    )
                    VALUES (
                        :semana,
                        :pm25_media,
                        :pm10_media,
                        :dias_risco,
                        :dias_validos,
                        :pct_risco,
                        :risco_ambiental
                    )
                    ON CONFLICT (semana) DO UPDATE SET
                        pm25_media      = EXCLUDED.pm25_media,
                        pm10_media      = EXCLUDED.pm10_media,
                        dias_risco      = EXCLUDED.dias_risco,
                        dias_validos    = EXCLUDED.dias_validos,
                        pct_risco       = EXCLUDED.pct_risco,
                        risco_ambiental = EXCLUDED.risco_ambiental;
                    """
                    ),
                    {
                        "semana": row.semana,
                        "pm25_media": row.pm25_media,
                        "pm10_media": row.pm10_media,
                        "dias_risco": row.dias_risco,
                        "dias_validos": row.dias_validos,
                        "pct_risco": row.pct_risco,
                        "risco_ambiental": risco,
                    },
                )

            log_pipeline_event(
                "gerar_relatorio",
                "success",
                f"Resumo semanal gerado para semana={semana}, risco_ambiental={risco}",
            )
        except Exception as e:
            log_pipeline_event(
                "gerar_relatorio",
                "failed",
                f"Erro ao gerar relatorio semana={semana}: {e}",
            )
            raise

    semanas = get_semanas()
    gerar_relatorio.expand(semana=semanas)