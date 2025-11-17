"""
Validação e carregamento seguro de variáveis de ambiente.
Garante que credenciais NUNCA sejam expostas com valores padrão.
"""
import os
import socket
from dotenv import load_dotenv

# Carregar .env uma única vez
load_dotenv()


def is_running_in_docker() -> bool:
    """
    Detecta se o código está rodando dentro de um container Docker.

    Returns:
        True se rodando em Docker, False se rodando localmente
    """
    # Método 1: Verificar se arquivo /.dockerenv existe (Docker sempre cria)
    if os.path.exists('/.dockerenv'):
        return True

    # Método 2: Verificar se hostname 'minio' resolve (só funciona dentro da rede Docker)
    try:
        socket.gethostbyname('minio')
        return True
    except socket.gaierror:
        return False


def get_required_env(var_name: str, error_message: str = None) -> str:
    """
    Obtém uma variável de ambiente obrigatória.

    Args:
        var_name: Nome da variável
        error_message: Mensagem de erro personalizada (opcional)

    Returns:
        Valor da variável

    Raises:
        ValueError: Se a variável não estiver configurada ou estiver vazia
    """
    value = os.getenv(var_name)

    if not value or value.strip() == "":
        if error_message:
            raise ValueError(error_message)
        raise ValueError(f"❌ Variável de ambiente obrigatória não configurada: {var_name}")

    return value.strip()


def get_optional_env(var_name: str) -> str:
    """
    Obtém uma variável de ambiente opcional.
    Retorna None ou string vazia se não configurada.

    Args:
        var_name: Nome da variável

    Returns:
        Valor da variável ou None
    """
    value = os.getenv(var_name)
    return value.strip() if value else None


# ==================== CONFIGURAÇÕES MINIO ====================

def get_minio_config() -> dict:
    """
    Obtém todas as configurações do MinIO.
    Detecta automaticamente se está rodando local ou em Docker e ajusta endpoint.

    Returns:
        Dict com 'endpoint', 'access_key', 'secret_key'

    Raises:
        ValueError: Se credenciais não estiverem configuradas
    """
    # Tentar ler MINIO_ENDPOINT de variável de ambiente
    endpoint = os.getenv("MINIO_ENDPOINT")

    # Auto-detecção: se não está em Docker, usar localhost ao invés de minio
    if not endpoint or endpoint.strip() == "":
        # Se não configurado, usar padrão baseado no ambiente
        endpoint = "minio:9000" if is_running_in_docker() else "localhost:9000"
    elif "minio:" in endpoint and not is_running_in_docker():
        # Se configurado com 'minio:' mas rodando localmente, trocar para localhost
        endpoint = endpoint.replace("minio:", "localhost:")
        print(f"ℹ️  Detectado execução local. Usando endpoint: {endpoint}")

    return {
        "endpoint": endpoint,
        "access_key": get_required_env("MINIO_ACCESS_KEY",
            "❌ MINIO_ACCESS_KEY não configurada. Configure variável de ambiente."),
        "secret_key": get_required_env("MINIO_SECRET_KEY",
            "❌ MINIO_SECRET_KEY não configurada. Configure variável de ambiente.")
    }


# ==================== CONFIGURAÇÕES LABEL STUDIO ====================

def get_labelstudio_config() -> dict:
    """
    Obtém todas as configurações do Label Studio.
    Detecta automaticamente se está rodando local ou em Docker e ajusta URL.

    Returns:
        Dict com 'url', 'token', 'project_id'

    Raises:
        ValueError: Se credenciais não estiverem configuradas
    """
    # Tentar ler LABELSTUDIO_URL de variável de ambiente
    url = os.getenv("LABELSTUDIO_URL")

    # Auto-detecção: se não está em Docker, usar localhost ao invés de label-studio
    if not url or url.strip() == "":
        url = "http://label-studio:8080" if is_running_in_docker() else "http://localhost:8001"
    elif "label-studio:" in url and not is_running_in_docker():
        # Se configurado com 'label-studio:' mas rodando localmente, trocar para localhost:8001
        url = url.replace("label-studio:8080", "localhost:8001")
        print(f"ℹ️  Detectado execução local. Usando Label Studio URL: {url}")

    return {
        "url": url,
        "token": get_required_env("LABELSTUDIO_TOKEN",
            "❌ LABELSTUDIO_TOKEN não configurada. Configure variável de ambiente."),
        "project_id": int(get_required_env("LABELSTUDIO_PROJECT",
            "❌ LABELSTUDIO_PROJECT não configurada. Configure variável de ambiente."))
    }
