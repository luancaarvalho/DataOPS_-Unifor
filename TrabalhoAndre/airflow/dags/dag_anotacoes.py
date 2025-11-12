from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
import os
from pathlib import Path
import boto3
from sqlalchemy import create_engine
from datetime import datetime
# import great_expectations as ge
import json
import logging

# Caminhos de dados
DEFAULT_DATA_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(DEFAULT_DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DEFAULT_DATA_DIR, "processed")

#Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["netflix"]
)
def fluxo_netflix():

    # ====================== ETL ======================

    @task()
    def listar_csvs():
        logger.info("📂 Listando arquivos CSV em %s", RAW_DIR)
        p = Path(RAW_DIR)
        arquivos = list(p.glob("*.csv"))
        if not arquivos:
            logger.error(" %d arquivos não encontrados")
            raise FileNotFoundError("Nenhum arquivo CSV encontrado em /opt/airflow/data/raw")
        logger.info("✅ %d arquivos encontrados", len(arquivos))
        return [str(c) for c in arquivos]  # Converte Path → string
        
    
    @task()
    def carregar_e_tratar(caminhos):
        logger.info("🚀 Iniciando tratamento dos dados")
        dfs = []
        for c in caminhos:
            df = pd.read_csv(c)
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)

        # ==== LIMPEZA E TRATAMENTO ====

        for col in ["director", "cast", "country", "rating"]:
            df[col] = df[col].fillna("Desconhecido")

        df["date_added"] = df["date_added"].fillna("Data desconhecida")

        mask_rating_is_duration = df["rating"].astype(str).str.contains(
            r"(min|Season|season|seasons)", case=False, na=False
        )
        df.loc[mask_rating_is_duration, "duration"] = df.loc[mask_rating_is_duration, "rating"]
        df.loc[mask_rating_is_duration, "rating"] = "Desconhecido"

        df["duration"] = df["duration"].fillna("Desconhecido")

        df["cast"] = df["cast"].apply(lambda x: [i.strip() for i in str(x).split(",")] if isinstance(x, str) else [])
        df["listed_in"] = df["listed_in"].apply(lambda x: [i.strip() for i in str(x).split(",")] if isinstance(x, str) else [])

        colunas_traduzidas = {
            "show_id": "id",
            "type": "tipo",
            "title": "titulo",
            "director": "diretor",
            "cast": "elenco",
            "country": "pais",
            "date_added": "data_adicao",
            "release_year": "ano_lancamento",
            "rating": "classificacao",
            "duration": "duracao",
            "listed_in": "categorias",
            "description": "descricao"
        }
        df = df.rename(columns=colunas_traduzidas)
        df = df.drop_duplicates(subset=["id"])
        logger.info("✅ Tratamento finalizado — %d registros", len(df))
        return df.to_json(orient="records")

    # @task()
    # 
    # def validar_dados_com_ge(json_data):
    #     """Validação simples de qualidade com Great Expectations"""

    #     df = pd.read_json(json_data, orient="records")
    #     gdf = ge.from_pandas(df)

    #     gdf.expect_column_values_to_not_be_null("titulo")
    #     gdf.expect_column_values_to_be_of_type("ano_lancamento", "int64")

    #     results = gdf.validate()

    #     if not results["success"]:
    #         raise ValueError("Falha na validação de dados com Great Expectations")

    #     return json_data 

    
    @task()
    def anotar_dados(json_data):
        logger.info("Criando anotacoes nos dados")
        df = pd.read_json(json_data, orient="records")
        ano_atual = datetime.now().year
        df["idade_filme"] = ano_atual - df["ano_lancamento"]

        def calcular_dias(data_str):
            try:
                if "Desconhecida" in data_str:
                    return None
                data = pd.to_datetime(data_str)
                return (datetime.now() - data).days
            except:
                return None
        df["dias_desde_adicao"] = df["data_adicao"].apply(calcular_dias)
        logger.info("Finalizando a criacao das anotacoes")
        return df.to_json(orient="records")


    
    @task()
    def salvar_parquet(json_data):
        logger.info("Salva os dados tratados como Parquet")
        df = pd.read_json(json_data, orient="records")
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        out_path = os.path.join(PROCESSED_DIR, "netflix_titles_tratado.parquet")
        df.to_parquet(out_path, index=False)
        logger.info("📂 Salvando arquivos parquet em %s", PROCESSED_DIR)
        return {"path": out_path, "linhas": len(df)}

    # ====================== LOADS ======================
    
    @task()
    def enviar_para_minio(info):
        logger.info("Envia o arquivo parquet tratado para o MinIO")
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # nome do serviço no docker-compose  
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        )

        bucket = "airflow-data"
        path_local = info["path"]
        nome_arquivo = os.path.basename(path_local)

        # Cria bucket se não existir
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)

        s3.upload_file(path_local, bucket, f"processed/{nome_arquivo}")
        return f"s3://{bucket}/processed/{nome_arquivo}"

    
    @task()
    def carregar_no_postgres(info):
        logger.info("Carrega o parquet tratado no banco Postgres")
        df = pd.read_parquet(info["path"])

        # Converter listas em strings antes de salvar
        df["elenco"] = df["elenco"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df["categorias"] = df["categorias"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))

        conn_str = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}"
        engine = create_engine(conn_str)

        df.to_sql("netflix_titles", engine, if_exists="replace", index=False)
        return "Tabela netflix_titles atualizada com sucesso!"
    
    #===================== RELATORIO =================
    
    @task(task_id="gerar_relatorio")
    def gerar_relatorio(**context):
        logger.info("📋 Gerando relatório de execução...")

        task_instances = context["ti"].get_dagrun().get_task_instances()
        relatorio = []

        for ti in task_instances:
            if ti.task_id != "gerar_relatorio":  # evita incluir ela mesma
                relatorio.append({
                    "task": ti.task_id,
                    "inicio": ti.start_date,
                    "fim": ti.end_date,
                    "duracao_s": ti.duration,  # usa o tempo oficial do Airflow
                    "estado": ti.state
                })

        df = pd.DataFrame(relatorio)
        logger.info("\n%s", df.to_string(index=False))
        return "Relatório final gerado com sucesso!"


    # ====================== DEPENDÊNCIAS ======================

    caminhos = listar_csvs()
    json = carregar_e_tratar(caminhos)
    # json_validado = validar_dados_com_ge(json)
    anotado = anotar_dados(json)
    resultado = salvar_parquet(anotado)
    enviar_para_minio(resultado)
    carregar_no_postgres(resultado)

    gerar_relatorio()



fluxo = fluxo_netflix()
