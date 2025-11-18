"""
Script para inserir dados na camada Bronze
Label Studio API → Bronze (dados brutos)
"""
import json
from datetime import datetime
from io import BytesIO
from minio import Minio
from label_studio_sdk import LabelStudio
from env_config import get_minio_config, get_labelstudio_config

def run():
    # Carregamento seguro de configurações (sem valores padrão para credenciais)
    labelstudio_config = get_labelstudio_config()
    minio_config = get_minio_config()

    LABEL_STUDIO_URL = labelstudio_config["url"]
    PROJECT_ID = labelstudio_config["project_id"]
    LABEL_STUDIO_API_KEY = labelstudio_config["token"]

    MINIO_ENDPOINT = minio_config["endpoint"]
    MINIO_ACCESS_KEY = minio_config["access_key"]
    MINIO_SECRET_KEY = minio_config["secret_key"]

    print("=" * 60)
    print(" Bronze: Inserindo Dados Brutos")
    print("=" * 60)

    # 1. Conectar à API do Label Studio
    print(f"\n Conectando ao Label Studio ({LABEL_STUDIO_URL})...")
    try:
        client = LabelStudio(base_url=LABEL_STUDIO_URL, api_key=LABEL_STUDIO_API_KEY)
        # Verificar conexão
        me = client.users.whoami()
        print(f" Autenticado como: {me.username}")
    except Exception as e:
        print(f" Erro de autenticação: {e}")
        raise

    # 2. Buscar dados do projeto
    print(f"\n Buscando tarefas do projeto {PROJECT_ID}...")
    try:
        tasks = client.tasks.list(project=PROJECT_ID)
        # Converter objetos Pydantic para dict com serialização JSON (converte datetime automaticamente)
        raw_data = [
            task.model_dump(mode='json') if hasattr(task, 'model_dump')
            else task.dict()
            for task in tasks
        ]
        print(f" ✓ {len(raw_data)} registros obtidos")
    except Exception as e:
        print(f" Erro: {e}")
        raise

    # 2. Upload para Bronze (dados brutos, sem limpeza)
    print(f"\n Salvando na camada Bronze...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"labelstudio_raw_{PROJECT_ID}_{timestamp}.json"

        json_data = json.dumps(raw_data, indent=2, ensure_ascii=False).encode('utf-8')
        json_stream = BytesIO(json_data)

        client.put_object(
            "bronze",
            filename,
            json_stream,
            len(json_data),
            content_type="application/json"
        )

        print(f" {filename} enviado para Bronze!")
        print(f"\n Resumo:")
        print(f"   • Registros: {len(raw_data)}")
        print(f"   • Bucket: bronze")
        print(f"   • Arquivo: {filename}")
        print("=" * 60)

        return {'status': 'success', 'records': len(raw_data), 'filename': filename}

    except Exception as e:
        print(f" Erro: {e}")
        raise

if __name__ == "__main__":
    run()
