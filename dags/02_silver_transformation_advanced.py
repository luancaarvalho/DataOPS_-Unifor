"""
DAG: Silver Transformation (Advanced - Data-Aware + Dynamic Task Mapping)
Usa TaskFlow API, Datasets, e Dynamic Task Mapping

Recursos Avançados:
- TaskFlow API (@task decorator)
- Airflow Datasets (dispara quando BRONZE_DATASET atualiza)
- Dynamic Task Mapping (processa chunks em paralelo)
- Outlets para notificar datasets downstream
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset
import sys
sys.path.insert(0, '/opt/airflow/scripts_pipeline')
from transform_silver import run as transform_silver
from datasets import BRONZE_DATASET, SILVER_DATASET
from pipeline_monitoring import PipelineMetrics

default_args = {
    'owner': 'dataops',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=20),
}

@dag(
    dag_id='silver_transformation_advanced',
    default_args=default_args,
    description='Silver: Data-Aware Transformation with Dynamic Task Mapping',
    schedule=[BRONZE_DATASET],  # Dispara automaticamente quando Bronze atualiza!
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['silver', 'advanced', 'data-aware', 'taskflow', 'dynamic-mapping'],
    doc_md="""
    # Silver Transformation DAG (Advanced)

    ## Objetivo
    Limpeza, validacao, padronizacao e enriquecimento de dados Bronze, com extracao de Named Entity Recognition (NER) de anotacoes Label Studio.

    ## Recursos Avançados
    - **Data-Aware Scheduling**: Dispara automaticamente quando BRONZE_DATASET é atualizado
    - **TaskFlow API**: Código limpo com decorators @task para melhor legibilidade
    - **Dynamic Task Mapping**: Processa dados em chunks paralelos para otimizacao
    - **NER Extraction**: Extrai entidades (CLIENTE, PRODUTO, VALOR, etc) de anotacoes
    - **Outlets**: Notifica SILVER_DATASET para disparar Gold automaticamente
    - **Multi-Source Input**: Integra Label Studio API com fallback para MinIO Bronze

    ## Fluxo de Execução
    1. **Acionado automaticamente** quando Bronze atualiza (disparo via BRONZE_DATASET)

    2. **Lê dados do Bronze**:
       - Arquivo mais recente de s3://bronze/labelstudio_raw_*.json
       - Contém estrutura completa: id, data, annotations com labels NER

    3. **Extrai Named Entities (NER)**:
       - CLIENTE: Nome do cliente da compra
       - PRODUTO: Nome do produto comprado
       - QUANTIDADE: Quantidade comprada (valores numericos)
       - CANAL: Canal de venda (Varejo Fisico, E-commerce, etc)
       - VALOR: Preco unitario da transacao
       - FORMA_PAGAMENTO: Metodo de pagamento (Cartao, Dinheiro, etc)
       - CIDADE: Localizacao de entrega
       - STATUS: Status do pedido (Entregue, Pendente, Cancelado)
       - AVALIACAO: Avaliacao numerica do cliente
       - SENTIMENTO: Sentimento extraido (Positivo, Negativo, Neutro)

    4. **Valida e limpa dados**:
       - Remove espacos em branco desnecessarios
       - Padroniza tipos de dados
       - Remove registros completamente vazios
       - Preserva registros com dados parciais (nao descarta)

    5. **Divide em chunks para processamento paralelo**:
       - id_validation: Valida IDs unicos
       - data_validation: Valida campos data
       - content_validation: Valida integridade de conteudo
       - Executa em paralelo via Dynamic Task Mapping

    6. **Consolida validacoes**:
       - Agrega resultados das tarefas paralelas
       - Registra problemas encontrados

    7. **Salva em MinIO Silver**:
       - Bucket: 'silver'
       - Formato: Parquet com compressao Snappy
       - Arquivo: silver_transformed_TIMESTAMP.parquet
       - Contém: dados limpos + campos NER extraidos + metricas

    8. **Atualiza SILVER_DATASET**:
       - Notifica Airflow que dados estao prontos
       - Dispara automaticamente gold_publishing_advanced DAG
       - Completa segunda etapa da cascata Bronze -> Silver -> Gold

    ## Contrato de Dados
    Entrada: JSON estruturado de s3://bronze/labelstudio_raw_*.json
    Saida: Parquet estruturado em s3://silver/silver_transformed_*.parquet
    Triggers: gold_publishing_advanced DAG via SILVER_DATASET

    ## Estrutura de Saida (Silver Parquet)
    Colunas mantidas do Bronze:
    - id: Identificador unico
    - data: Data da transacao

    Colunas extraidas via NER:
    - cliente: Nome do cliente (STRING)
    - produto: Nome do produto (STRING)
    - quantidade: Quantidade comprada (INTEGER)
    - valor: Preco unitario (FLOAT)
    - canal: Canal de venda (STRING)
    - forma_pagamento: Metodo de pagamento (STRING)
    - cidade: Localizacao (STRING)
    - status: Status do pedido (STRING)
    - avaliacao: Score de avaliacao (FLOAT)
    - sentimento: Sentimento do cliente (STRING)

    ## Configuracao
    - Schedule: Dispara quando BRONZE_DATASET atualiza (data-aware scheduling)
    - Timeout: 20 minutos por execucao
    - Retries: 3 tentativas com backoff exponencial (max 10 minutos)
    - Owner: dataops
    - Tags: silver, advanced, data-aware, taskflow, dynamic-mapping

    ## Monitoramento
    Métricas rastreadas via PipelineMetrics:
    - Registros validos processados
    - Registros removidos (inválidos)
    - Taxa de limpeza (% removido)
    - Arquivo gerado e localizacao
    - Timestamp de execucao
    - Problemas detectados em validacoes paralelas
    - Status: success, warning, ou failure
    """,
)
def silver_transformation_advanced():
    """Pipeline de transformação Silver com Dynamic Task Mapping"""

    @task
    def get_validation_chunks():
        """
        Gera lista de chunks para processamento paralelo

        Retorna lista de dicts com parâmetros de validação
        Cada chunk pode ser processado independentemente
        """
        # Exemplo: dividir validação em diferentes tipos
        validation_types = [
            {'type': 'id_validation', 'description': 'Valida IDs'},
            {'type': 'data_validation', 'description': 'Valida campo data'},
            {'type': 'content_validation', 'description': 'Valida conteúdo'},
        ]
        return validation_types

    @task(outlets=[SILVER_DATASET], multiple_outputs=True)
    def transform_and_validate(**context):
        """
        Transforma dados Bronze → Silver

        Returns:
            dict: Métricas da transformação
        """
        PipelineMetrics.log_task_start('transform_and_validate', 'SILVER')

        try:
            result = transform_silver()

            # Calcular taxa de limpeza
            total = result.get('records', 0) + result.get('removed', 0)
            cleanup_rate = (result.get('removed', 0) / total * 100) if total > 0 else 0

            metrics = {
                'status': result.get('status'),
                'records_valid': result.get('records'),
                'records_removed': result.get('removed'),
                'cleanup_rate': f"{cleanup_rate:.1f}%",
                'filename': result.get('filename'),
                'dataset': str(SILVER_DATASET),
                'timestamp': datetime.now().isoformat()
            }

            # Adicionar stats detalhadas se disponível
            if 'stats' in result:
                metrics['validation_stats'] = result['stats']

            PipelineMetrics.log_task_success('transform_and_validate', 'SILVER', metrics)
            return metrics

        except Exception as e:
            PipelineMetrics.log_task_failure('transform_and_validate', 'SILVER', e)
            raise

    @task
    def validate_chunk(chunk: dict, transformation_result: dict):
        """
        Valida um chunk específico de dados

        Esta tarefa usa Dynamic Task Mapping para executar em paralelo

        Args:
            chunk: Tipo de validação a realizar
            transformation_result: Resultado da transformação
        """
        import time

        print(f"🔍 Validando: {chunk['description']}")

        # Simular validação (substituir por lógica real se necessário)
        stats = transformation_result.get('validation_stats', {})

        if chunk['type'] == 'id_validation':
            issues = stats.get('id_null', 0) + stats.get('id_missing', 0)
            print(f"   • Problemas de ID: {issues}")

        elif chunk['type'] == 'data_validation':
            issues = stats.get('data_null', 0) + stats.get('data_missing', 0) + stats.get('data_empty', 0)
            print(f"   • Problemas de data: {issues}")

        elif chunk['type'] == 'content_validation':
            issues = stats.get('data_all_values_empty', 0)
            print(f"   • Problemas de conteúdo: {issues}")

        return {
            'chunk_type': chunk['type'],
            'status': 'validated',
            'issues_found': issues
        }

    @task
    def consolidate_validations(validation_results: list):
        """
        Consolida resultados de todas as validações paralelas

        Args:
            validation_results: Lista de resultados das validações (do expand)
        """
        # Converter para lista explícita (resolve LazyXComSelectSequence)
        validation_list = list(validation_results) if validation_results else []

        print("=" * 80)
        print("CONSOLIDANDO VALIDAÇÕES")

        total_issues = sum(r['issues_found'] for r in validation_list)

        for result in validation_list:
            print(f"✓ {result['chunk_type']}: {result['issues_found']} problemas")

        print(f"\n Total de problemas detectados: {total_issues}")
        print("=" * 80)

        # Retornar como dict JSON-serializable
        return {
            'total_issues': total_issues,
            'validations': [{'chunk_type': r['chunk_type'], 'issues_found': r['issues_found']} for r in validation_list]
        }

    @task
    def log_completion(transformation_result: dict, consolidation: dict):
        """
        Log final com resumo da execução

        Args:
            transformation_result: Resultado da transformação
            consolidation: Resultado consolidado das validações
        """
        print("=" * 80)
        print("SILVER TRANSFORMATION CONCLUIDA")
        print(f"Registros validos: {transformation_result['records_valid']}")
        print(f"Registros removidos: {transformation_result['records_removed']}")
        print(f"Taxa de limpeza: {transformation_result['cleanup_rate']}")
        print(f"Arquivo: {transformation_result['filename']}")
        print(f"Validacoes paralelas: {len(consolidation['validations'])}")
        print(f"Dataset atualizado: {transformation_result['dataset']}")
        print("Gold DAG sera disparado automaticamente")
        print("=" * 80)

    # Definir fluxo com Dynamic Task Mapping
    chunks = get_validation_chunks()
    transformation = transform_and_validate()

    # Dynamic Task Mapping: executa validate_chunk() para cada chunk em paralelo!
    validations = validate_chunk.expand(chunk=chunks, transformation_result=[transformation])

    consolidation = consolidate_validations(validations)
    log_completion(transformation, consolidation)

# Instanciar DAG
silver_dag = silver_transformation_advanced()
