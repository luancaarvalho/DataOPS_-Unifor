# MinIO Setup - Aula 03 (Arquitetura Medallion)

## Sobre o MinIO

MinIO é um servidor de armazenamento de objetos de alto desempenho compatível com a API S3 da Amazon. É ideal para armazenamento de dados não estruturados como fotos, vídeos, arquivos de log, backups e dados analíticos.

Esta configuração implementa a **Arquitetura Medallion** (Bronze → Silver → Gold) para organização de dados em camadas.

## Versão

- **MinIO Server**: RELEASE.2025-10-15T17-29-55Z (Latest - com correções de segurança)
- **MinIO Client (mc)**: Latest

## Configuração

### Portas

- **9010**: API S3 (acesso programático)
- **9011**: Console Web (interface gráfica)

⚠️ **Nota**: As portas foram alteradas para evitar conflitos com a aula_02.

### Credenciais Padrão

- **Usuário**: `minioadmin`
- **Senha**: `minioadmin`

⚠️ **IMPORTANTE**: Altere estas credenciais em produção!

## Arquitetura Medallion - Buckets

O setup cria automaticamente 8 buckets seguindo a arquitetura medallion e boas práticas:

### Camadas de Dados (Medallion)

1. **bronze**: Dados brutos, sem transformação (zona landing)
2. **silver**: Dados limpos, validados e enriquecidos
3. **gold**: Dados agregados e prontos para consumo analítico

### Buckets Auxiliares

4. **raw-data**: Dados originais antes do processamento
5. **staging**: Dados temporários durante processamento
6. **archive**: Dados históricos/arquivados
7. **ml-models**: Modelos de Machine Learning e artefatos
8. **logs**: Logs de processos e auditoria

## Fluxo de Dados Recomendado

```
raw-data → bronze → silver → gold → BI/Analytics
                 ↓
              staging (temporário)
                 ↓
              archive (histórico)
```

## Como Usar

### Subir o MinIO

```bash
# Na pasta minio
docker-compose up -d
```

### Verificar Status

```bash
docker-compose ps
```

### Acessar Console Web

Abra no navegador: http://localhost:9011

- Usuário: `minioadmin`
- Senha: `minioadmin`

### Ver Logs

```bash
docker-compose logs -f minio
```

### Parar o MinIO

```bash
docker-compose down
```

### Remover Tudo (incluindo dados)

```bash
docker-compose down -v
```

## Usando o MinIO Client (mc)

### Configurar alias local

```bash
docker exec -it minio-client-aula03 sh

# Dentro do container
mc alias set myminio http://minio:9000 minioadmin minioadmin
```

### Comandos Úteis por Camada

```bash
# BRONZE - Ingestão de dados brutos
mc cp dados_brutos.csv myminio/bronze/vendas/2025/10/

# SILVER - Dados limpos
mc cp dados_limpos.parquet myminio/silver/vendas/2025/10/

# GOLD - Dados agregados
mc cp metricas_vendas.parquet myminio/gold/vendas/monthly/

# STAGING - Processamento temporário
mc cp temp_processing.csv myminio/staging/job_123/

# ARCHIVE - Arquivamento
mc cp myminio/bronze/vendas/2024/ myminio/archive/vendas/2024/ --recursive

# LOGS - Auditoria
mc cp pipeline_log.json myminio/logs/airflow/2025-10-31/
```

## Usando Python (boto3) com Arquitetura Medallion

```python
import boto3
from botocore.client import Config
from datetime import datetime

# Configurar cliente S3
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Exemplo: Pipeline Bronze → Silver → Gold
def pipeline_medallion():
    today = datetime.now().strftime('%Y/%m/%d')
    
    # 1. Ingerir dados brutos (BRONZE)
    s3_client.upload_file(
        'raw_data.csv',
        'bronze',
        f'vendas/{today}/raw_data.csv'
    )
    
    # 2. Processar e limpar (SILVER)
    # ... processamento ...
    s3_client.upload_file(
        'cleaned_data.parquet',
        'silver',
        f'vendas/{today}/cleaned_data.parquet'
    )
    
    # 3. Agregar e preparar para BI (GOLD)
    # ... agregação ...
    s3_client.upload_file(
        'aggregated_metrics.parquet',
        'gold',
        f'vendas/monthly/{today}/metrics.parquet'
    )
    
    # 4. Log do processo
    s3_client.upload_file(
        'pipeline_log.json',
        'logs',
        f'pipeline/{today}/execution_log.json'
    )

# Listar arquivos por camada
def list_layer(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        print(f"{bucket_name}/{obj['Key']}")

# Usar
list_layer('bronze')
list_layer('silver')
list_layer('gold')
```

## Integração com Airflow (Arquitetura Medallion)

```python
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

@dag(
    dag_id='medallion_pipeline',
    start_date=datetime(2025, 10, 31),
    schedule='@daily',
    catchup=False
)
def medallion_data_pipeline():
    
    @task
    def ingest_to_bronze():
        """Ingestão de dados brutos na camada Bronze"""
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_file(
            filename='raw_data.csv',
            key=f'vendas/{datetime.now().strftime("%Y/%m/%d")}/raw.csv',
            bucket_name='bronze'
        )
    
    @task
    def transform_to_silver():
        """Limpeza e validação para camada Silver"""
        # Processar dados
        pass
    
    @task
    def aggregate_to_gold():
        """Agregação para camada Gold"""
        # Agregar dados
        pass
    
    # Pipeline
    bronze = ingest_to_bronze()
    silver = transform_to_silver()
    gold = aggregate_to_gold()
    
    bronze >> silver >> gold

medallion_data_pipeline()
```

## Boas Práticas - Organização de Dados

### Estrutura de Pastas Recomendada

```
bronze/
  ├── vendas/
  │   └── 2025/10/31/
  │       └── raw_data.csv
  └── clientes/
      └── 2025/10/31/
          └── raw_data.json

silver/
  ├── vendas/
  │   └── 2025/10/31/
  │       └── cleaned_data.parquet
  └── clientes/
      └── 2025/10/31/
          └── validated_data.parquet

gold/
  ├── vendas/
  │   └── monthly/
  │       └── 2025-10/
  │           └── metrics.parquet
  └── clientes/
      └── aggregated/
          └── customer_360.parquet
```

## Troubleshooting

### Porta já em uso

Se as portas 9010 ou 9011 já estiverem em uso, edite o `docker-compose.yml`:

```yaml
ports:
  - "9020:9000"  # API
  - "9021:9001"  # Console
```

### Conflito com Aula 02

Esta configuração usa portas diferentes (9010/9011) para permitir rodar simultaneamente com a aula_02 (9000/9001).

### Permissões de volume

Se tiver problemas com permissões:

```bash
docker-compose down -v
docker volume prune -f
docker-compose up -d
```

## Monitoramento e Observabilidade

### Métricas de Armazenamento

```bash
# Tamanho por bucket
mc du myminio/bronze
mc du myminio/silver
mc du myminio/gold

# Contagem de objetos
mc ls --recursive myminio/bronze | wc -l
```

### Logs de Acesso

Os logs de acesso podem ser configurados para serem enviados para o bucket `logs`:

```bash
mc admin config set myminio logger_webhook:1 endpoint=http://webhook-server
```

## Recursos Adicionais

- [Documentação Oficial do MinIO](https://min.io/docs/minio/linux/index.html)
- [Arquitetura Medallion (Databricks)](https://www.databricks.com/glossary/medallion-architecture)
- [MinIO Client (mc) Guide](https://min.io/docs/minio/linux/reference/minio-mc.html)
- [S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
