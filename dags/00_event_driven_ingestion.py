"""
DAG: Event-Driven Ingestion (MinIO)
Monitora bucket 'inbox' no MinIO e dispara pipeline automaticamente

Recursos Avançados:
- Sensor Deferrable (baixo consumo de recursos)
- Event-driven (dispara quando novo arquivo aparece)
- Dynamic Task Mapping (processa múltiplos arquivos em paralelo)
- TaskFlow API
- Datasets (dispara pipeline completo Bronze → Silver → Gold)
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
import sys

sys.path.insert(0, '/opt/airflow/scripts_pipeline')
sys.path.insert(0, '/opt/airflow/dags')

from sensors.s3_deferrable_sensor import S3PrefixSensorDeferrable
from datasets import BRONZE_DATASET, NEW_FILE_DATASET
from pipeline_monitoring import PipelineMetrics

default_args = {
    'owner': 'dataops',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='event_driven_ingestion',
    default_args=default_args,
    description='Event-Driven: Monitora MinIO inbox e dispara pipeline automaticamente',
    schedule=None,  # ⚡ Sem schedule - dispara imediatamente após conclusão!
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['event-driven', 'minio', 'sensors', 'deferrable', 'advanced'],
    max_active_runs=10,  # Permite até 10 instâncias em paralelo
    doc_md="""
    # Event-Driven Ingestion DAG

    ##  Como Funciona

    Este DAG monitora **continuamente e imediatamente** o bucket **'inbox'** no MinIO.
    Quando um novo arquivo é detectado, o pipeline completo é disparado automaticamente!
    **Após processar um evento, imediatamente inicia uma nova monitoração** (verdadeiramente contínuo!).

    ## Fluxo Event-Driven

    ```
    1. Monitora bucket 'inbox' (sensor deferrable)
       └─ Libera worker slots durante espera
       └─ Consome mínimo de recursos

    2. Detecta novo arquivo
       └─ Lista todos os arquivos novos
       └─ Prepara para processamento paralelo

    3. Processa arquivos (Dynamic Task Mapping)
       └─ Múltiplos arquivos processados em paralelo
       └─ Move arquivos de inbox → bronze

    4. Dispara Pipeline Bronze
       └─ Bronze → Silver → Gold (via Datasets)
       └─ Processamento completo automatizado
    ```

    ## Buckets MinIO

    - **inbox**: Arquivos novos (staging)
    - **bronze**: Dados brutos (após ingestão)
    - **silver**: Dados limpos
    - **gold**: Dados otimizados (Parquet)

    ## Como Testar

    1. Fazer upload de arquivo para MinIO bucket 'inbox'
    2. DAG detecta automaticamente
    3. Pipeline completo executa end-to-end
    4. Verificar dados em gold/

    ## Configuração

    - **Schedule**: Sem schedule fixo (dispara imediatamente após conclusão)
    - **Sensor**: Verifica a cada 30 segundos
    - **Deferrable**: Sim (baixo consumo, libera worker slots)
    - **Paralelo**: Sim (Dynamic Task Mapping)
    - **Auto-trigger**: Bronze DAG (via Datasets BRONZE_DATASET)
    - **Loop Contínuo**: Sim! Re-dispara imediatamente após conclusão
    """,
)
def event_driven_ingestion():
    """Pipeline event-driven para MinIO"""

    # Sensor Deferrable - monitora novos arquivos
    wait_for_files = S3PrefixSensorDeferrable(
        task_id='wait_for_new_files_in_inbox',
        bucket_name='inbox',
        prefix='',  # Qualquer arquivo
        check_interval=30,  # Verifica a cada 30 segundos
        timeout=3600 * 24,  # Timeout: 24 horas
        mode='reschedule',  # Modo deferrable
        poke_interval=30,
    )

    @task
    def list_new_files(**context):
        """
        Lista todos os arquivos detectados no inbox

        Returns:
            list: Lista de nomes de arquivos
        """
        from minio import Minio
        from env_config import get_minio_config

        # Conectar ao MinIO com configurações seguras
        minio_config = get_minio_config()
        client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )

        try:
            # Listar objetos no inbox
            objects = list(client.list_objects('inbox', recursive=True))
            files = [obj.object_name for obj in objects]

            print(f"📁 Arquivos detectados no inbox: {len(files)}")
            for file in files:
                print(f"   • {file}")

            # Se vazio, retornar lista vazia (não falha)
            return files if files else []

        except Exception as e:
            print(f"⚠️  Erro ao listar inbox: {e}")
            return []

    @task
    def process_file(filename: str):
        """
        Processa um arquivo individual do inbox

        Esta função usa Dynamic Task Mapping para executar em paralelo!

        Args:
            filename: Nome do arquivo a processar
        """
        from minio import Minio
        import json
        from env_config import get_minio_config

        print(f" Processando arquivo: {filename}")

        # Conectar ao MinIO com configurações seguras
        minio_config = get_minio_config()
        client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )

        try:
            # 1. Baixar arquivo do inbox
            response = client.get_object('inbox', filename)
            data = response.read()

            # 2. Validar se é JSON válido
            try:
                json_data = json.loads(data.decode('utf-8'))
                record_count = len(json_data) if isinstance(json_data, list) else 1
                print(f"   ✓ JSON válido: {record_count} registros")
            except:
                print(f"    Arquivo não é JSON válido, processando como binário")
                record_count = 0

            # 3. Mover para bronze (copiar + deletar do inbox)
            from io import BytesIO
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            bronze_filename = f"event_driven_{timestamp}_{filename}"

            client.put_object(
                'bronze',
                bronze_filename,
                BytesIO(data),
                len(data),
                content_type='application/json'
            )

            # 4. Remover do inbox após sucesso
            client.remove_object('inbox', filename)

            print(f"  Movido para bronze: {bronze_filename}")

            return {
                'original_file': filename,
                'bronze_file': bronze_filename,
                'records': record_count,
                'status': 'success'
            }

        except Exception as e:
            print(f"  Erro ao processar: {e}")
            return {
                'original_file': filename,
                'status': 'error',
                'error': str(e)
            }

    @task(outlets=[BRONZE_DATASET])  # ⚡ Atualiza BRONZE_DATASET ao finalizar
    def consolidate_results(process_results: list):
        """
        Consolida resultados de todos os arquivos processados

        Args:
            process_results: Lista de resultados do processamento (do expand)
        """
        # Converter para lista explícita (resolve LazyXComSelectSequence)
        results_list = list(process_results) if process_results else []

        print("=" * 80)
        print("CONSOLIDANDO RESULTADOS EVENT-DRIVEN")

        successful = [r for r in results_list if r.get('status') == 'success']
        failed = [r for r in results_list if r.get('status') == 'error']
        total_records = sum(r.get('records', 0) for r in successful)

        print(f"\n Arquivos processados com sucesso: {len(successful)}")
        print(f" Arquivos com erro: {len(failed)}")
        print(f" Total de registros: {total_records}")

        if successful:
            print(f"\n Arquivos em Bronze:")
            for result in successful:
                print(f"   • {result['bronze_file']} ({result['records']} registros)")

        if failed:
            print(f"\n  Arquivos com erro:")
            for result in failed:
                print(f"   • {result['original_file']}: {result.get('error')}")

        print("=" * 80)

        # Retornar como dict JSON-serializable
        return {
            'successful': len(successful),
            'failed': len(failed),
            'total_records': total_records,
            'bronze_files': [r.get('bronze_file', '') for r in successful]
        }

    @task
    def log_event_completion(consolidation: dict):
        """
        Log final e preparação para disparar pipeline Bronze

        Args:
            consolidation: Resultado consolidado
        """
        print("=" * 80)
        print(" EVENT-DRIVEN INGESTION CONCLUÍDA")
        print(f" Arquivos processados: {consolidation['successful']}")
        print(f" Registros totais: {consolidation['total_records']}")
        print("")
        print(" Próximos Passos:")
        print("   1. Dados movidos para Bronze ✓")
        print("   2. Pipeline Bronze será executado (via Datasets BRONZE_DATASET)")
        print("   3. Silver será disparado automaticamente (via Datasets SILVER_DATASET)")
        print("   4. Gold será disparado automaticamente (via Datasets GOLD_DATASET)")
        print("")
        print(" Re-disparando monitoração imediatamente...")
        print("=" * 80)
        return "completed"

    # Task para re-disparar o próprio DAG imediatamente após conclusão
    trigger_next_run = TriggerDagRunOperator(
        task_id='trigger_next_event_driven_run',
        trigger_dag_id='event_driven_ingestion',
        trigger_rule='all_done',  # Sempre dispara, mesmo com sucesso ou falha parcial
        reset_dag_run=False,  # Não reseta runs anteriores
        wait_for_completion=False,  # Não espera a próxima conclusão (fire and forget)
    )

    # Trigger Bronze DAG (se houver arquivos processados)
    # Nota: Bronze DAG avançado usa datasets, então será disparado automaticamente
    # quando BRONZE_DATASET for atualizado

    # Definir fluxo com Dynamic Task Mapping
    files = list_new_files()

    #  Dynamic Task Mapping: processa cada arquivo em paralelo!
    processed = process_file.expand(filename=files)

    consolidation = consolidate_results(processed)
    completion = log_event_completion(consolidation)

    #  Re-dispara imediatamente após conclusão (loop contínuo!)
    completion >> trigger_next_run

    # Dependências
    wait_for_files >> files

# Instanciar DAG
event_dag = event_driven_ingestion()
