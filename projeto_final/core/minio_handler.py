# -*- coding: utf-8 -*-
"""
MinIO Handler
--------------
Funções utilitárias para interação com o MinIO:
- Criar bucket caso não exista
- Fazer upload de arquivos
- Baixar arquivos
- Descobrir o arquivo mais recente em um bucket

Usado pelas DAGs Bronze e Silver.
"""

from minio import Minio
import os


# =====================================================
# 🔧 CONFIGURAÇÃO DO CLIENTE
# =====================================================
MINIO_URL = "minio:9000"        # Nome do serviço no docker-compose
MINIO_ACCESS = "admin"
MINIO_SECRET = "admin123"

client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS,
    secret_key=MINIO_SECRET,
    secure=False
)


# =====================================================
# 🪣 UTILITÁRIOS DE BUCKET
# =====================================================
def ensure_bucket(bucket_name: str):
    """Cria o bucket se não existir"""
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print(f"🪣 Bucket criado: {bucket_name}")
    else:
        print(f"🪣 Bucket já existe: {bucket_name}")


# =====================================================
# 📤 UPLOAD DE ARQUIVOS
# =====================================================
def upload_file(local_path: str, bucket_name: str, object_name: str):
    """Envia arquivo local para um bucket MinIO"""
    ensure_bucket(bucket_name)

    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=local_path
    )

    print(f"⬆ Upload concluído → {bucket_name}/{object_name}")


# =====================================================
# 📥 DOWNLOAD DE ARQUIVOS
# =====================================================
def download_file(bucket_name: str, object_name: str, local_path: str):
    """Baixa arquivo do MinIO para o Airflow"""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    client.fget_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=local_path
    )

    print(f"⬇ Download concluído → {local_path}")


# =====================================================
# 🔎 PEGAR O ARQUIVO MAIS RECENTE DO BUCKET
# =====================================================
def get_latest_file(bucket_name: str):
    """
    Retorna o objeto mais recente do bucket com base na data de modificação.
    """
    objects = list(client.list_objects(bucket_name))

    if not objects:
        raise FileNotFoundError(f"Nenhum arquivo encontrado no bucket '{bucket_name}'")

    # Ordenar pelo campo last_modified
    objects_sorted = sorted(objects, key=lambda obj: obj.last_modified, reverse=True)

    latest = objects_sorted[0]

    print(f"📌 Último arquivo encontrado no bucket '{bucket_name}': {latest.object_name}")

    return latest
