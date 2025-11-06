"""
Script 1: Gerenciamento de Ambientes de Diversos Prompts no Langfuse

Este script demonstra como gerenciar diferentes prompts no Langfuse,
criando, listando e organizando prompts para diferentes ambientes.

Integracao com LM Studio usando o modelo Gemma 2 34B.
"""

import os
from langfuse import Langfuse, observe, get_client
from openai import OpenAI
from dotenv import load_dotenv

# Carregar variaveis de ambiente
load_dotenv()

# Inicializar Langfuse
langfuse = Langfuse(
    public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
    secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
    host=os.getenv("LANGFUSE_HOST", "http://localhost:3000")
)

# Configurar cliente OpenAI para usar LM Studio
client = OpenAI(
    base_url=os.getenv("LM_STUDIO_HOST", "http://localhost:1234") + "/v1",
    api_key="not-needed"  # LM Studio nao requer API key
)


def criar_prompts_ambiente():
    """
    Cria diferentes prompts para diversos ambientes e casos de uso.
    """
    prompts = [
        {
            "name": "analise_dados_dev",
            "type": "text",
            "prompt": "Voce e um assistente especializado em analise de dados. Analise os seguintes dados e forneca insights: {{dados}}",
            "config": {
                "model": "gemma-3-4b",
                "temperature": 0.7,
                "max_tokens": 500
            },
            "labels": ["dev"],
            "tags": ["desenvolvimento", "analise", "dados"]
        },
        {
            "name": "analise_dados_prod",
            "type": "text",
            "prompt": "Voce e um especialista em analise de dados empresariais. Forneca uma analise detalhada e profissional dos seguintes dados: {{dados}}",
            "config": {
                "model": "gemma-3-4b",
                "temperature": 0.3,  # Mais conservador em producao
                "max_tokens": 1000
            },
            "labels": ["production"],
            "tags": ["producao", "analise", "dados"]
        },
        {
            "name": "sumarizacao_texto",
            "type": "text",
            "prompt": "Faca um resumo conciso e objetivo do seguinte texto: {{texto}}",
            "config": {
                "model": "gemma-3-4b",
                "temperature": 0.5,
                "max_tokens": 300
            },
            "labels": ["production"],
            "tags": ["producao", "sumarizacao"]
        },
        {
            "name": "geracao_codigo",
            "type": "text",
            "prompt": "Voce e um programador experiente. Gere codigo Python limpo e bem documentado para: {{requisito}}",
            "config": {
                "model": "gemma-3-4b",
                "temperature": 0.2,
                "max_tokens": 800
            },
            "labels": ["dev"],
            "tags": ["desenvolvimento", "codigo"]
        }
    ]
    
    print("=" * 80)
    print("CRIANDO PROMPTS NO LANGFUSE")
    print("=" * 80)
    
    for prompt_data in prompts:
        try:
            # Criar prompt no Langfuse usando a API v3
            langfuse.create_prompt(
                name=prompt_data["name"],
                type=prompt_data["type"],
                prompt=prompt_data["prompt"],
                config=prompt_data["config"],
                labels=prompt_data["labels"],
                tags=prompt_data["tags"]
            )
            print(f"\n[OK] Prompt criado: {prompt_data['name']}")
            print(f"  - Labels: {', '.join(prompt_data['labels'])}")
            print(f"  - Tags: {', '.join(prompt_data['tags'])}")
        except Exception as e:
            print(f"\n[ERRO] Ao criar prompt {prompt_data['name']}: {e}")


def listar_prompts():
    """
    Lista todos os prompts cadastrados no Langfuse.
    """
    print("\n" + "=" * 80)
    print("LISTANDO PROMPTS CADASTRADOS")
    print("=" * 80)
    
    try:
        # No Langfuse v3, listar prompts requer usar a API diretamente
        print("\n[INFO] Para listar prompts, acesse: http://localhost:3000")
        print("       Ou use langfuse.get_prompt('nome_do_prompt')")
        
    except Exception as e:
        print(f"\n[ERRO] Ao listar prompts: {e}")


@observe(as_type="generation")
def testar_prompt_com_lmstudio(prompt_name: str, variables: dict):
    """
    Testa um prompt especifico usando o LM Studio.
    
    Args:
        prompt_name: Nome do prompt no Langfuse
        variables: Dicionario com as variaveis para preencher o prompt
    """
    print("\n" + "=" * 80)
    print(f"TESTANDO PROMPT: {prompt_name}")
    print("=" * 80)
    
    try:
        # Buscar prompt do Langfuse usando API v3 (buscar a versao mais recente)
        prompt = langfuse.get_prompt(prompt_name, label="latest")
        
        # Compilar prompt com variaveis
        prompt_text = prompt.compile(**variables)
        
        print(f"\nPrompt compilado:\n{prompt_text}")
        print("\n" + "-" * 80)
        print("Enviando para LM Studio...")
        print("-" * 80)
        
        # Fazer chamada para LM Studio
        response = client.chat.completions.create(
            model=os.getenv("LM_STUDIO_MODEL", "gemma-3-4b"),
            messages=[
                {"role": "user", "content": prompt_text}
            ],
            temperature=prompt.config.get("temperature", 0.7),
            max_tokens=prompt.config.get("max_tokens", 500)
        )
        
        output = response.choices[0].message.content
        
        print(f"\nResposta do modelo:\n{output}")
        print(f"\nTokens usados: {response.usage.total_tokens}")
        print("\n[OK] Interacao registrada no Langfuse!")
        
        return output
        
    except Exception as e:
        print(f"\n[ERRO] Ao testar prompt: {e}")
        return None


def main():
    """
    Funcao principal para demonstracao.
    """
    print("\n" + "=" * 80)
    print("LANGFUSE - GERENCIAMENTO DE PROMPTS EM DIFERENTES AMBIENTES")
    print("=" * 80)
    
    # 1. Criar prompts
    print("\n1. Criando prompts...")
    criar_prompts_ambiente()
    
    # 2. Listar prompts
    print("\n2. Listando prompts...")
    listar_prompts()
    
    # 3. Testar um prompt com LM Studio
    print("\n3. Testando prompt com LM Studio...")
    print("   (Certifique-se de que o LM Studio esta rodando)")
    
    try:
        resultado = testar_prompt_com_lmstudio(
            prompt_name="analise_dados_dev",
            variables={
                "dados": "Vendas Q1 2024: R$ 150.000, Q2 2024: R$ 180.000, Q3 2024: R$ 165.000"
            }
        )
        
        # Flush para garantir que tudo foi enviado
        langfuse.flush()
        
    except Exception as e:
        print(f"\n[AVISO] Nao foi possivel testar com LM Studio: {e}")
        print("        Certifique-se de que o LM Studio esta rodando")
    
    print("\n" + "=" * 80)
    print("SCRIPT FINALIZADO")
    print("=" * 80)
    print("\nAcesse http://localhost:3000 para visualizar os prompts no Langfuse Dashboard")


if __name__ == "__main__":
    main()
