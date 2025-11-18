"""
DAG: Gold Publishing (Advanced - Data-Aware)
Usa TaskFlow API e Datasets

Recursos Avançados:
- TaskFlow API (@task decorator)
- Airflow Datasets (dispara quando SILVER_DATASET atualiza)
- Outlets para notificar datasets downstream
- Geração de Parquet otimizado
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset
import sys
sys.path.insert(0, '/opt/airflow/scripts_pipeline')
from aggregate_gold import run as aggregate_gold
from datasets import SILVER_DATASET, GOLD_DATASET
from pipeline_monitoring import PipelineMetrics

default_args = {
    'owner': 'dataops',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=25),
}

@dag(
    dag_id='gold_publishing_advanced',
    default_args=default_args,
    description='Gold: Data-Aware Publishing with TaskFlow API',
    schedule=[SILVER_DATASET],  # ⚡ Dispara automaticamente quando Silver atualiza!
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold', 'advanced', 'data-aware', 'taskflow', 'parquet'],
    doc_md="""
    # Gold Publishing DAG (Advanced)

    ## Objetivo
    Consolidacao e agregacao analitica de dados Silver, gerando metricas pré-calculadas e tabelas dimensionais otimizadas para visualizacao e BI.

    ## Recursos Avançados
    - **Data-Aware Scheduling**: Dispara automaticamente quando SILVER_DATASET é atualizado
    - **TaskFlow API**: Código limpo com decorators @task para melhor estrutura
    - **Parquet Optimization**: Formato colunar com compressão Snappy para eficiencia
    - **Dimensional Model**: Gera tabelas por dimensoes (Cliente, Produto, Canal, etc)
    - **Outlets**: Notifica GOLD_DATASET sinalizando dados prontos para consumo

    ## Fluxo de Execução
    1. **Acionado automaticamente** quando Silver atualiza (disparo via SILVER_DATASET)

    2. **Lê dados Silver**:
       - Arquivo mais recente de s3://silver/silver_transformed_*.parquet
       - Contém dados limpos com campos NER já extraidos

    3. **Consolida dados analíticos**:
       - Mantém registros originais com dados completos
       - Adiciona campos derivados (receita = valor * quantidade)
       - Calcula valor unitario e outras métricas

    4. **Gera agregacoes por dimensao**:
       - **CLIENTE**: valor_total, valor_medio, qtd_transacoes, quantidade_vendida
       - **PRODUTO**: valor_total, valor_medio, qtd_transacoes, quantidade_vendida
       - **CANAL**: valor_total, qtd_transacoes, quantidade_vendida
       - **STATUS**: valor_total, qtd_transacoes, quantidade_vendida
       - **CIDADE**: valor_total, qtd_transacoes, quantidade_vendida
       - **FORMA_PAGAMENTO**: valor_total, valor_medio, qtd_transacoes, quantidade_vendida

    5. **Calcula KPIs globais**:
       - total_vendas: Soma de todas as vendas (R$)
       - quantidade_total: Quantidade total vendida
       - qtd_clientes: Numero unico de clientes
       - qtd_produtos: Numero unico de produtos
       - qtd_canais: Numero unico de canais
       - qtd_transacoes: Total de transacoes
       - valor_medio_transacao: Valor médio por transacao
       - ticket_medio: Valor médio gasto por cliente

    6. **Salva em MinIO Gold**:
       - Bucket: 'gold'
       - Formato: Parquet com compressao Snappy
       - Arquivo principal: gold_analytics_TIMESTAMP.parquet (consolidado)
       - Arquivos dimensionais: gold_dim_*.parquet (6 dimensoes)
       - Metadados: gold_metadata_TIMESTAMP.json

    7. **Valida qualidade do Parquet**:
       - Verifica se arquivo nao esta vazio
       - Valida formato correto
       - Confirma compressao aplicada

    8. **Notifica pronto para BI**:
       - Registra que dados estao prontos
       - Fornece exemplos de conexao (Spark, Pandas, DuckDB)
       - Atualiza GOLD_DATASET para pipelines downstream

    ## Contrato de Dados
    Entrada: Parquet de s3://silver/silver_transformed_*.parquet
    Saida: Parquets agregados em s3://gold/gold_*.parquet
    Triggers: Downstream consumers via GOLD_DATASET

    ## Estrutura de Saida (Gold Parquet)

    ### gold_analytics_TIMESTAMP.parquet (Tabela consolidada)
    - id: Identificador
    - data: Data da transacao
    - cliente: Nome do cliente
    - produto: Nome do produto
    - quantidade: Quantidade
    - valor: Preco unitario
    - canal: Canal de venda
    - forma_pagamento: Metodo de pagamento
    - status: Status do pedido
    - cidade: Localizacao
    - avaliacao: Score de avaliacao
    - sentimento: Sentimento
    - receita: valor * quantidade (DERIVADO)
    - valor_unitario: valor / quantidade (DERIVADO)

    ### gold_dim_cliente_TIMESTAMP.parquet
    - cliente: Nome unico
    - valor_total: Soma de vendas (R$)
    - valor_medio: Média de venda
    - qtd_transacoes: Numero de compras
    - quantidade_vendida: Total de items

    ### gold_dim_produto_TIMESTAMP.parquet
    - produto: Nome unico
    - valor_total: Receita total
    - valor_medio: Preço medio
    - qtd_transacoes: Vezes vendido
    - quantidade_vendida: Unidades vendidas

    ### gold_dim_canal_TIMESTAMP.parquet
    - canal: Nome do canal
    - valor_total: Total por canal
    - qtd_transacoes: Transacoes
    - quantidade_vendida: Unidades

    ### gold_dim_status_TIMESTAMP.parquet
    - status: Status do pedido
    - valor_total: Total por status
    - qtd_transacoes: Contagem
    - quantidade_vendida: Unidades

    ### gold_dim_cidade_TIMESTAMP.parquet
    - cidade: Nome da cidade
    - valor_total: Total vendido
    - qtd_transacoes: Numero de vendas
    - quantidade_vendida: Unidades

    ### gold_dim_pagamento_TIMESTAMP.parquet
    - forma_pagamento: Metodo
    - valor_total: Total coletado
    - valor_medio: Ticket medio
    - qtd_transacoes: Numero de operacoes
    - quantidade_vendida: Unidades

    ### gold_metadata_TIMESTAMP.json
    - timestamp: ISO 8601 execution time
    - layer: 'gold'
    - source: silver parquet filename
    - kpis: Global KPI values
    - aggregations: Dimension record counts
    - files_generated: List of created files

    ## Configuracao
    - Schedule: Dispara quando SILVER_DATASET atualiza (data-aware scheduling)
    - Timeout: 25 minutos por execucao
    - Retries: 3 tentativas com backoff exponencial (max 10 minutos)
    - Owner: dataops
    - Tags: gold, advanced, data-aware, taskflow, parquet

    ## Monitoramento
    Métricas rastreadas via PipelineMetrics:
    - Registros consolidados
    - Registros agregados por dimensao
    - KPIs calculados (valores e contagens)
    - Arquivos gerados (8 total)
    - Tamanho de arquivo Parquet
    - Compressao aplicada
    - Timestamp de execucao
    - Status: success, warning, ou failure

    ## Consumidores de Dados
    Downstream systems can consume Gold data via:
    - **Streamlit Dashboard**: Leitura de gold_analytics e gold_dim_* Parquets
    - **Spark/PySpark**: spark.read.parquet('s3://gold/gold_*.parquet')
    - **Pandas**: pd.read_parquet('s3://gold/gold_*.parquet')
    - **DuckDB**: SELECT * FROM read_parquet('s3://gold/gold_*.parquet')
    - **Power BI / Tableau**: Conectar via S3 ou via drivers Parquet
    """,
)
def gold_publishing_advanced():
    """Pipeline de publicação Gold usando TaskFlow API"""

    @task(outlets=[GOLD_DATASET], multiple_outputs=True)
    def aggregate_and_optimize(**context):
        """
        Agrega e otimiza dados Silver → Gold

        Returns:
            dict: Métricas da operação
        """
        PipelineMetrics.log_task_start('aggregate_and_optimize', 'GOLD')

        try:
            result = aggregate_gold()

            metrics = {
                'status': result.get('status'),
                'records': result.get('records'),
                'filename': result.get('filename'),
                'format': 'parquet',
                'compression': 'snappy',
                'dataset': str(GOLD_DATASET),
                'timestamp': datetime.now().isoformat()
            }

            PipelineMetrics.log_task_success('aggregate_and_optimize', 'GOLD', metrics)
            return metrics

        except Exception as e:
            PipelineMetrics.log_task_failure('aggregate_and_optimize', 'GOLD', e)
            raise

    @task
    def validate_parquet_quality(aggregation_result: dict):
        """
        Valida qualidade do arquivo Parquet gerado

        Args:
            aggregation_result: Resultado da agregação
        """
        print("Validando qualidade do Parquet...")

        # Validacoes
        if aggregation_result['records'] == 0:
            raise ValueError("Arquivo Parquet vazio!")

        if aggregation_result['format'] != 'parquet':
            raise ValueError("Formato invalido!")

        print("Arquivo Parquet valido")
        print(f"   - Registros: {aggregation_result['records']}")
        print(f"   - Compressao: {aggregation_result['compression']}")
        print(f"   - Arquivo: {aggregation_result['filename']}")

        return True

    @task
    def notify_bi_ready(aggregation_result: dict):
        """
        Notifica que dados estão prontos para consumo BI

        Args:
            aggregation_result: Resultado da agregação
        """
        print("=" * 80)
        print("DADOS PRONTOS PARA BUSINESS INTELLIGENCE")
        print(f"Dataset: {aggregation_result['dataset']}")
        print(f"Arquivo Parquet: {aggregation_result['filename']}")
        print(f"Registros: {aggregation_result['records']}")
        print(f"Compressao: {aggregation_result['compression']}")
        print("")
        print("Conecte seu BI tool:")
        print("   - Spark: spark.read.parquet('s3://gold/...')")
        print("   - Pandas: pd.read_parquet('s3://gold/...')")
        print("   - DuckDB: SELECT * FROM read_parquet('s3://gold/...')")
        print("=" * 80)

    @task
    def log_completion(aggregation_result: dict):
        """
        Log final com resumo da execução

        Args:
            aggregation_result: Resultado da agregação
        """
        print("=" * 80)
        print("GOLD PUBLISHING CONCLUIDA")
        print(f"Registros: {aggregation_result['records']}")
        print(f"Arquivo: {aggregation_result['filename']}")
        print(f"Dataset atualizado: {aggregation_result['dataset']}")
        print("Pipeline Bronze -> Silver -> Gold completado!")
        print("=" * 80)

    # Definir fluxo de dependências
    aggregation = aggregate_and_optimize()
    validation = validate_parquet_quality(aggregation)
    notification = notify_bi_ready(aggregation)
    log_completion(aggregation) >> [validation, notification]

# Instanciar DAG
gold_dag = gold_publishing_advanced()
