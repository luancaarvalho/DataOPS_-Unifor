"""
PyAirbyte Script SIMPLIFICADO: GitHub to MinIO
===============================================

Este é um exemplo simplificado e direto ao ponto.
Ideal para testes rápidos e aprendizado.
"""

import airbyte as ab
import os

# Configurações
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "your_token_here")
REPO = "luancaarvalho/DataOPS_-Unifor"  # Repositório a ser extraído

print("🚀 Iniciando pipeline PyAirbyte...")
print(f"📦 Repositório: {REPO}")

# 1. Configurar Source (GitHub)
print("\n📥 Configurando GitHub source...")
source = ab.get_source(
    "source-github",
    config={
        "credentials": {"personal_access_token": GITHUB_TOKEN},
        "repositories": [REPO],
        "start_date": "2024-10-01T00:00:00Z",
    },
    install_if_missing=True,
)

# 2. Verificar conexão
print("🔍 Verificando conexão...")
source.check()
print("✅ GitHub conectado!")

# 3. Selecionar streams
print("\n📋 Selecionando streams...")
source.select_streams(["issues", "pull_requests", "stargazers"])
print("✅ Streams selecionados!")

# 4. Extrair dados
print("\n📥 Extraindo dados do GitHub...")
result = source.read()

# 5. Exibir resultados
print("\n📊 Dados extraídos:")
for stream_name, records in result.streams.items():
    record_list = list(records)
    print(f"  • {stream_name}: {len(record_list)} registros")

# 6. Configurar Destination (MinIO)
print("\n💾 Configurando MinIO destination...")
destination = ab.get_destination(
    "destination-s3",
    config={
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "s3_bucket_name": "bronze",
        "s3_bucket_path": "github_simple",
        "s3_bucket_region": "us-east-1",
        "s3_endpoint": "http://host.docker.internal:9010",  # Porta correta para Docker
        "format": {"format_type": "JSONL", "compression": {"compression_type": "GZIP"}},
    },
    install_if_missing=True,
)

# 7. Verificar conexão
print("🔍 Verificando conexão com MinIO...")
destination.check()
print("✅ MinIO conectado!")

# 8. Carregar dados
print("\n📤 Carregando dados no MinIO...")
destination.write(result)

print("\n✅ Pipeline concluído com sucesso!")
print(f"🌐 Acesse: http://localhost:9011")
print(f"📂 Bucket: bronze/github_simple")
