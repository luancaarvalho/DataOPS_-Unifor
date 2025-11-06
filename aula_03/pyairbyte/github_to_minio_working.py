"""
PyAirbyte: GitHub to MinIO (usando DuckDB como intermediário)
=============================================================
Como o conector S3 tem problemas com networking Docker,
vamos salvar primeiro no DuckDB e depois exportar para MinIO
"""

import airbyte as ab
import os
import sys
import boto3
import gzip
import json
from pathlib import Path
from datetime import datetime

# Configurações
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPO", "luancaarvalho/DataOPS_-Unifor")

# Validar credenciais
if not GITHUB_TOKEN:
    print("❌ Erro: GITHUB_TOKEN não configurado!")
    print("Configure a variável de ambiente GITHUB_TOKEN ou crie um arquivo .env")
    print("Veja .env.example para um template")
    sys.exit(1)

print("🚀 Iniciando pipeline PyAirbyte - GitHub to MinIO")
print(f"📦 Repositório: {REPO}")
print("="*60)

# 1. Configurar Source (GitHub)
print("\n📥 Configurando GitHub source...")
source = ab.get_source(
    "source-github",
    config={
        "credentials": {"personal_access_token": GITHUB_TOKEN},
        "repositories": [REPO],
        "start_date": "2024-01-01T00:00:00Z",
    },
    install_if_missing=True,
)

# 2. Verificar conexão
print("🔍 Verificando conexão com GitHub...")
source.check()
print("✅ GitHub conectado!")

# 3. Selecionar streams
print("\n📋 Selecionando streams...")
source.select_streams(["issues", "pull_requests", "stargazers", "commits"])
print("✅ Streams selecionados!")

# 4. Extrair dados para DuckDB Cache (padrão do PyAirbyte)
print("\n📥 Extraindo dados do GitHub para cache local...")
result = source.read()

# 5. Exibir resultados
print("\n📊 Dados extraídos:")
stream_counts = {}
for stream_name, records in result.streams.items():
    record_list = list(records)
    count = len(record_list)
    stream_counts[stream_name] = count
    print(f"  • {stream_name}: {count} registros")

# 6. Exportar para MinIO usando boto3
print("\n💾 Enviando dados para MinIO...")

# Configurar cliente S3 (boto3) para MinIO
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# Verificar conexão com MinIO
try:
    s3_client.head_bucket(Bucket='bronze')
    print("✅ MinIO conectado!")
except Exception as e:
    print(f"❌ Erro ao conectar com MinIO: {e}")
    exit(1)

# Criar diretório temporário
temp_dir = Path("/tmp/pyairbyte_export")
temp_dir.mkdir(exist_ok=True)

# Exportar cada stream para MinIO
timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
uploaded_files = []

for stream_name, records in result.streams.items():
    record_list = list(records)
    
    if len(record_list) == 0:
        print(f"  ⏭️  Pulando {stream_name} (vazio)")
        continue
    
    # Salvar como JSONL comprimido
    filename = f"{stream_name}_{timestamp}.jsonl.gz"
    local_path = temp_dir / filename
    s3_key = f"github_data/{stream_name}/{filename}"
    
    # Escrever arquivo JSONL comprimido
    with gzip.open(local_path, 'wt', encoding='utf-8') as f:
        for record in record_list:
            # Converter record para dict se necessário
            if hasattr(record, '__dict__'):
                record_dict = record.__dict__
            elif hasattr(record, 'to_dict'):
                record_dict = record.to_dict()
            else:
                record_dict = dict(record) if not isinstance(record, dict) else record
            
            # Adicionar metadados Airbyte
            airbyte_record = {
                "_airbyte_raw_id": str(record_dict.get("_airbyte_raw_id", "")),
                "_airbyte_extracted_at": str(record_dict.get("_airbyte_extracted_at", "")),
                "_airbyte_data": record_dict
            }
            f.write(json.dumps(airbyte_record, default=str) + '\n')
    
    # Upload para MinIO
    try:
        s3_client.upload_file(
            str(local_path),
            'bronze',
            s3_key
        )
        uploaded_files.append(s3_key)
        print(f"  ✅ {stream_name}: {len(record_list)} registros → bronze/{s3_key}")
    except Exception as e:
        print(f"  ❌ Erro ao enviar {stream_name}: {e}")
    
    # Limpar arquivo temporário
    local_path.unlink()

# Limpar diretório temporário
temp_dir.rmdir()

# Resumo final
print("\n" + "="*60)
print("✅ Pipeline concluído com sucesso!")
print("="*60)
print(f"\n📊 Resumo:")
print(f"  • Total de streams: {len(stream_counts)}")
print(f"  • Total de registros: {sum(stream_counts.values())}")
print(f"  • Arquivos enviados: {len(uploaded_files)}")
print(f"\n🌐 Acesse: http://localhost:9011")
print(f"📂 Bucket: bronze/github_data/")
print(f"\n💡 Streams processados:")
for stream, count in stream_counts.items():
    if count > 0:
        print(f"  • {stream}: {count} registros")
