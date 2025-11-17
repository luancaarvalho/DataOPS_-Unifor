"""
Módulo de Monitoramento do Pipeline
Logs, métricas, callbacks e notificações
"""
import json
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


class PipelineMetrics:
    """Gerenciador de métricas do pipeline"""

    @staticmethod
    def log_task_start(task_id, layer):
        """Log de início de tarefa"""
        logger.info("=" * 80)
        logger.info(f"INICIO DA TAREFA: {task_id}")
        logger.info(f"Camada: {layer}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 80)

    @staticmethod
    def log_task_success(task_id, layer, metrics):
        """Log de sucesso com métricas"""
        logger.info("=" * 80)
        logger.info(f"TAREFA CONCLUIDA COM SUCESSO: {task_id}")
        logger.info(f"Camada: {layer}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("")
        logger.info("METRICAS:")
        for key, value in metrics.items():
            logger.info(f"  • {key}: {value}")
        logger.info("=" * 80)

    @staticmethod
    def log_task_failure(task_id, layer, error):
        """Log de falha"""
        logger.error("=" * 80)
        logger.error(f"TAREFA FALHOU: {task_id}")
        logger.error(f"Camada: {layer}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        logger.error(f"Erro: {str(error)}")
        logger.error("=" * 80)

    @staticmethod
    def log_retry(task_id, retry_number):
        """Log de retry"""
        logger.warning("=" * 80)
        logger.warning(f"TENTATIVA DE REEXECUCAO: {task_id}")
        logger.warning(f"Numero de tentativa: {retry_number}")
        logger.warning(f"Timestamp: {datetime.now().isoformat()}")
        logger.warning("=" * 80)


def on_success_callback(context):
    """Callback executado quando tarefa é bem-sucedida"""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = context['dag'].dag_id

    # Extrair métricas do XCom (se disponível)
    metrics = task_instance.xcom_pull(task_ids=task_id, key='return_value')

    logger.info("=" * 80)
    logger.info(f"CALLBACK DE SUCESSO")
    logger.info(f"DAG: {dag_id}")
    logger.info(f"Task: {task_id}")
    logger.info(f"Execution Date: {context['execution_date']}")
    logger.info(f"Duration: {task_instance.duration}s")

    if metrics:
        logger.info("")
        logger.info("Metricas da tarefa:")
        logger.info(json.dumps(metrics, indent=2, ensure_ascii=False))

    logger.info("=" * 80)


def on_failure_callback(context):
    """Callback executado quando tarefa falha"""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = context['dag'].dag_id
    exception = context.get('exception')

    logger.error("=" * 80)
    logger.error(f"CALLBACK DE FALHA")
    logger.error(f"DAG: {dag_id}")
    logger.error(f"Task: {task_id}")
    logger.error(f"Execution Date: {context['execution_date']}")
    logger.error(f"Try Number: {task_instance.try_number}")
    logger.error(f"Max Tries: {task_instance.max_tries}")

    if exception:
        logger.error("")
        logger.error(f"Excecao: {str(exception)}")
        logger.error(f"Tipo: {type(exception).__name__}")

    logger.error("=" * 80)


def on_retry_callback(context):
    """Callback executado quando tarefa é retentada"""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = context['dag'].dag_id

    logger.warning("=" * 80)
    logger.warning(f"CALLBACK DE RETRY")
    logger.warning(f"DAG: {dag_id}")
    logger.warning(f"Task: {task_id}")
    logger.warning(f"Execution Date: {context['execution_date']}")
    logger.warning(f"Try Number: {task_instance.try_number}")
    logger.warning(f"Max Tries: {task_instance.max_tries}")
    logger.warning(f"Proxima tentativa em: {task_instance.next_retry_datetime()}")
    logger.warning("=" * 80)


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback executado quando SLA é violado"""
    logger.error("=" * 80)
    logger.error(f"SLA VIOLADO!")
    logger.error(f"DAG: {dag.dag_id}")
    logger.error(f"Tasks que violaram SLA: {[task.task_id for task in task_list]}")
    logger.error(f"Tasks bloqueadoras: {[task.task_id for task in blocking_task_list]}")
    logger.error("=" * 80)
