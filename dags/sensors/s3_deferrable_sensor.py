"""
Sensores Deferrable para MinIO/S3
Reduz consumo de recursos durante espera por eventos

Sensores Deferrable liberam o worker slot durante a espera,
permitindo maior eficiência de recursos.
"""
from datetime import timedelta
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.decorators import apply_defaults
from minio import Minio
from env_config import get_required_env


class S3KeySensorDeferrable(BaseSensorOperator):
    """
    Sensor deferrable para detectar novos arquivos em MinIO/S3

    Este sensor:
    - Libera worker slot enquanto aguarda
    - Usa trigger para reativação assíncrona
    - Consome menos recursos que sensor tradicional
    """

    template_fields = ['bucket_name', 'object_key']

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        object_key: str,
        minio_endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        check_interval: int = 30,  # segundos
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_key = object_key
        # Require environment variables - no default credentials
        self.minio_endpoint = minio_endpoint or get_required_env('MINIO_ENDPOINT')
        self.access_key = access_key or get_required_env('MINIO_ACCESS_KEY')
        self.secret_key = secret_key or get_required_env('MINIO_SECRET_KEY')
        self.check_interval = check_interval

    def poke(self, context):
        """
        Verifica se o arquivo existe no bucket

        Returns:
            bool: True se arquivo existe, False caso contrário
        """
        self.log.info(f"Verificando arquivo: s3://{self.bucket_name}/{self.object_key}")

        try:
            client = Minio(
                self.minio_endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )

            # Tentar obter metadata do objeto
            client.stat_object(self.bucket_name, self.object_key)

            self.log.info(f"✅ Arquivo encontrado: {self.object_key}")
            return True

        except Exception as e:
            self.log.info(f"⏳ Arquivo não encontrado ainda: {self.object_key}")
            return False

    def execute(self, context):
        """
        Execução deferrable - libera worker slot durante espera
        """
        if not self.poke(context):
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):
        """
        Callback após trigger - verifica novamente
        """
        if not self.poke(context):
            # Se ainda não encontrou, adia novamente
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
                method_name="execute_complete",
            )
        self.log.info("✅ Sensor deferrable concluído - arquivo detectado")


class S3PrefixSensorDeferrable(BaseSensorOperator):
    """
    Sensor deferrable para detectar novos arquivos por prefixo

    Útil para detectar qualquer arquivo novo em uma pasta
    """

    template_fields = ['bucket_name', 'prefix']

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        prefix: str = '',
        minio_endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        check_interval: int = 30,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        # Require environment variables - no default credentials
        self.minio_endpoint = minio_endpoint or get_required_env('MINIO_ENDPOINT')
        self.access_key = access_key or get_required_env('MINIO_ACCESS_KEY')
        self.secret_key = secret_key or get_required_env('MINIO_SECRET_KEY')
        self.check_interval = check_interval

    def poke(self, context):
        """
        Verifica se existem arquivos com o prefixo

        Returns:
            bool: True se encontrou arquivos, False caso contrário
        """
        self.log.info(f"Verificando arquivos com prefixo: s3://{self.bucket_name}/{self.prefix}")

        try:
            client = Minio(
                self.minio_endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )

            # Listar objetos com o prefixo
            objects = list(client.list_objects(
                self.bucket_name,
                prefix=self.prefix,
                recursive=True
            ))

            if objects:
                self.log.info(f"✅ Encontrados {len(objects)} arquivo(s)")
                # Salvar lista de arquivos no XCom para tarefas downstream
                context['task_instance'].xcom_push(
                    key='detected_files',
                    value=[obj.object_name for obj in objects]
                )
                return True
            else:
                self.log.info(f"⏳ Nenhum arquivo encontrado ainda")
                return False

        except Exception as e:
            self.log.warning(f"Erro ao verificar bucket: {e}")
            return False

    def execute(self, context):
        """
        Execução deferrable - libera worker slot durante espera
        """
        if not self.poke(context):
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):
        """
        Callback após trigger - verifica novamente
        """
        if not self.poke(context):
            # Se ainda não encontrou, adia novamente
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=self.check_interval)),
                method_name="execute_complete",
            )
        self.log.info("✅ Sensor deferrable concluído - arquivo(s) detectado(s)")
