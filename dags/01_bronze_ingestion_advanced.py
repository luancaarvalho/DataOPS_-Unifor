"""
DAG: Bronze Ingestion (Advanced - Data-Aware)
Usa TaskFlow API, Datasets, e Deferrable Sensors

Recursos Avançados:
- TaskFlow API (@task decorator)
- Airflow Datasets (data-aware scheduling)
- Outlets para notificar datasets downstream
- Logging estruturado
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset
import sys
sys.path.insert(0, '/opt/airflow/scripts_pipeline')
from insert_bronze import run as insert_bronze
from datasets import BRONZE_DATASET
from pipeline_monitoring import PipelineMetrics

default_args = {
    'owner': 'dataops',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id='bronze_ingestion_advanced',
    default_args=default_args,
    description='Bronze: Data-Aware Ingestion with TaskFlow API',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bronze', 'advanced', 'data-aware', 'taskflow'],
    doc_md="""
    # Bronze Ingestion DAG (Advanced)

    ## Objetivo
    Ingesta de dados brutos do Label Studio com fallback para MinIO Bronze, iniciando o pipeline de dados.

    ## Recursos Avançados
    - **TaskFlow API**: Decorators @task para código mais limpo e legível
    - **Airflow Datasets**: Notifica downstream automaticamente quando dados são atualizados
    - **Data-Aware Scheduling**: DAGs Silver e Gold disparam automaticamente em cascata
    - **Outlets**: Dataset BRONZE_DATASET é atualizado ao final para ativar dependências
    - **Multi-Auth Fallback**: Tenta Bearer, Token scheme, e sem autenticacao com Label Studio

    ## Fluxo de Execução
    1. **Extrai dados do Label Studio API** com multi-autenticacao:
       - Primeira tentativa: Bearer token (JWT refresh_token ou access_token)
       - Segunda tentativa: Token scheme (DRF token authentication)
       - Terceira tentativa: Sem autenticacao (modo publico)
       - Fallback: Lê arquivo JSON mais recente em MinIO Bronze se todas as tentativas falharem

    2. **Valida conexão e autenticação**:
       - Verifica credenciais e conectividade com Label Studio
       - Registra tentativas e falhas com detalhes de debug

    3. **Salva dados em MinIO**:
       - Bucket: 'bronze'
       - Formato: JSON com estrutura completa (id, data, annotations)
       - Arquivo: labelstudio_raw_PROJECT_ID_TIMESTAMP.json

    4. **Atualiza BRONZE_DATASET**:
       - Notifica Airflow que dados foram atualizados
       - Dispara automaticamente Silver Transformation DAG
       - Inicia cascata de transformacoes Bronze -> Silver -> Gold

    ## Contrato de Dados
    Entrada: Array JSON do Label Studio ou arquivo bronze antigo
    Saida: JSON estruturado em s3://bronze/labelstudio_raw_*.json
    Triggera: silver_transformation_advanced DAG via BRONZE_DATASET

    ## Configuracao
    - Schedule: Diariamente (@daily)
    - Timeout: 30 minutos por execucao
    - Retries: 3 tentativas com backoff exponencial (max 10 minutos)
    - Owner: dataops
    - Tags: bronze, advanced, data-aware, taskflow

    ## Monitoramento
    Métricas rastreadas via PipelineMetrics:
    - Número de registros extraidos
    - Arquivo gerado e localizacao
    - Timestamp de execucao
    - Status: success, warning, ou failure
    """,
)
def bronze_ingestion_advanced():
    """Pipeline de ingestão Bronze usando TaskFlow API"""

    @task(
        outlets=[BRONZE_DATASET],  # Notifica que BRONZE_DATASET foi atualizado
        multiple_outputs=True,
    )
    def extract_and_load(**context):
        """
        Extrai dados do Label Studio e carrega na Bronze

        Returns:
            dict: Métricas da operação
        """
        PipelineMetrics.log_task_start('extract_and_load', 'BRONZE')

        try:
            result = insert_bronze()

            metrics = {
                'status': result.get('status'),
                'records': result.get('records'),
                'filename': result.get('filename'),
                'dataset': str(BRONZE_DATASET),
                'timestamp': datetime.now().isoformat()
            }

            PipelineMetrics.log_task_success('extract_and_load', 'BRONZE', metrics)
            return metrics

        except Exception as e:
            PipelineMetrics.log_task_failure('extract_and_load', 'BRONZE', e)
            raise

    @task
    def validate_extraction(extraction_result: dict):
        """
        Valida se a extração foi bem-sucedida

        Args:
            extraction_result: Resultado da tarefa anterior
        """
        if extraction_result['status'] != 'success':
            raise ValueError(f"Extração falhou: {extraction_result}")

        if extraction_result['records'] == 0:
            raise ValueError("Nenhum registro foi extraído!")

        print(f"[OK] Validação OK: {extraction_result['records']} registros extraídos")
        return True

    @task
    def log_completion(extraction_result: dict):
        """
        Log final com resumo da execução

        Args:
            extraction_result: Resultado da extração
        """
        print("=" * 80)
        print("BRONZE INGESTION CONCLUIDA")
        print(f"Registros: {extraction_result['records']}")
        print(f"Arquivo: {extraction_result['filename']}")
        print(f"Dataset atualizado: {extraction_result['dataset']}")
        print("Silver DAG sera disparado automaticamente")
        print("=" * 80)

    # Definir fluxo de dependências
    extraction = extract_and_load()
    validation = validate_extraction(extraction)
    log_completion(extraction) >> validation

# Instanciar DAG
bronze_dag = bronze_ingestion_advanced()
