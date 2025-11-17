"""
Airflow Datasets - Data-Aware Scheduling
Definição de datasets para orquestração data-driven
"""
from airflow.datasets import Dataset

# Datasets representam artefatos de dados no pipeline
# Quando um dataset é atualizado, DAGs dependentes são automaticamente acionados

# Bronze: Dados brutos do Label Studio
BRONZE_DATASET = Dataset("s3://bronze/labelstudio_raw")

# Silver: Dados limpos e validados
SILVER_DATASET = Dataset("s3://silver/labelstudio_clean")

# Gold: Dados otimizados em Parquet
GOLD_DATASET = Dataset("s3://gold/labelstudio_gold")

# Event-driven: Novos arquivos no MinIO
NEW_FILE_DATASET = Dataset("s3://inbox/new_files")

# Metadata sobre os datasets
DATASET_INFO = {
    "bronze": {
        "uri": "s3://bronze/labelstudio_raw",
        "description": "Dados brutos do Label Studio API",
        "format": "JSON",
        "schema": "raw",
        "retention": "90 days"
    },
    "silver": {
        "uri": "s3://silver/labelstudio_clean",
        "description": "Dados limpos e validados",
        "format": "JSON",
        "schema": "validated",
        "retention": "180 days"
    },
    "gold": {
        "uri": "s3://gold/labelstudio_gold",
        "description": "Dados otimizados para análise",
        "format": "Parquet",
        "schema": "normalized",
        "retention": "365 days"
    }
}
