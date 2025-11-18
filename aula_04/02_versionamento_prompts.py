"""
Script 2: Versionamento de Prompts no Langfuse

Este script demonstra como versionar prompts no Langfuse, permitindo:
- Criar novas versoes de prompts existentes
- Comparar diferentes versoes
- Testar e validar mudancas antes de promover para producao
- Rastrear o historico de mudancas

Integracao com LM Studio usando o modelo Gemma.
"""

import os
from langfuse import Langfuse, observe
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime

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
    api_key="not-needed"
)


def criar_prompt_inicial():
    """
    Cria a versao inicial de um prompt para demonstracao de versionamento.
    """
    print("=" * 80)
    print("CRIANDO PROMPT INICIAL (v1)")
    print("=" * 80)
    
    try:
        langfuse.create_prompt(
            name="assistente_cliente",
            type="text",
            prompt="Voce e um assistente de atendimento ao cliente. Responda de forma educada: {{pergunta}}",
            config={
                "model": "gemma-3-4b",
                "temperature": 0.7,
                "max_tokens": 300
            },
            labels=["v1"],
            tags=["atendimento", "cliente", "inicial"]
        )
        
        print("\n[OK] Prompt inicial criado com sucesso!")
        print("  Nome: assistente_cliente")
        print("  Versao: 1")
        return True
    
    except Exception as e:
        print(f"\n[ERRO] Ao criar prompt: {e}")
        return False


def criar_nova_versao(prompt_name: str, novo_texto: str, nova_config: dict, tags: list):
    """
    Cria uma nova versao de um prompt existente.
    
    Args:
        prompt_name: Nome do prompt a ser versionado
        novo_texto: Novo texto do prompt
        nova_config: Nova configuracao (temperatura, max_tokens, etc)
        tags: Tags para identificar a versao
    """
    print("\n" + "=" * 80)
    print(f"CRIANDO NOVA VERSAO DO PROMPT: {prompt_name}")
    print("=" * 80)
    
    try:
        # Criar nova versao do prompt
        langfuse.create_prompt(
            name=prompt_name,
            type="text",
            prompt=novo_texto,
            config=nova_config,
            labels=[tags[0]],  # Primeira tag como label
            tags=tags
        )
        
        print(f"\n[OK] Nova versao criada com sucesso!")
        print(f"  Tags: {', '.join(tags)}")
        
        return True
    
    except Exception as e:
        print(f"\n[ERRO] Ao criar nova versao: {e}")
        return False


def listar_versoes(prompt_name: str):
    """
    Lista todas as versoes de um prompt especifico.
    
    Args:
        prompt_name: Nome do prompt
    """
    print("\n" + "=" * 80)
    print(f"HISTORICO DE VERSOES: {prompt_name}")
    print("=" * 80)
    
    try:
        print(f"\n[INFO] Para ver o historico completo de versoes:")
        print(f"       Acesse: http://localhost:3000")
        print(f"       Va em: Prompts -> {prompt_name}")
        
        # Buscar versao production
        try:
            prompt_prod = langfuse.get_prompt(prompt_name, label="production")
            print(f"\n[OK] Versao PRODUCTION encontrada:")
            print(f"     Prompt: {prompt_prod.prompt[:80]}...")
        except:
            print(f"\n[INFO] Nenhuma versao marcada como 'production' ainda")
        
        # Buscar versao latest
        try:
            prompt_latest = langfuse.get_prompt(prompt_name, label="latest")
            print(f"\n[OK] Versao LATEST encontrada:")
            print(f"     Prompt: {prompt_latest.prompt[:80]}...")
        except:
            print(f"\n[INFO] Nenhuma versao marcada como 'latest'")
        
    except Exception as e:
        print(f"\n[ERRO] Ao listar versoes: {e}")


@observe(as_type="generation")
def comparar_versoes(prompt_name: str, label1: str, label2: str, pergunta_teste: str):
    """
    Compara duas versoes de um prompt testando com a mesma entrada.
    
    Args:
        prompt_name: Nome do prompt
        label1: Label da primeira versao (ex: 'v1', 'production')
        label2: Label da segunda versao (ex: 'v2', 'latest')
        pergunta_teste: Pergunta de teste para usar em ambas as versoes
    """
    print("\n" + "=" * 80)
    print(f"COMPARANDO VERSOES: {label1} vs {label2}")
    print("=" * 80)
    
    try:
        # Buscar as duas versoes
        prompt_v1 = langfuse.get_prompt(prompt_name, label=label1)
        prompt_v2 = langfuse.get_prompt(prompt_name, label=label2)
        
        print(f"\nPergunta de teste: {pergunta_teste}")
        print("\n" + "-" * 80)
        
        # Testar versao 1
        print(f"\nTESTANDO VERSAO {label1}")
        print("-" * 80)
        resultado_v1 = testar_versao(prompt_v1, {"pergunta": pergunta_teste}, label1)
        
        # Testar versao 2
        print(f"\nTESTANDO VERSAO {label2}")
        print("-" * 80)
        resultado_v2 = testar_versao(prompt_v2, {"pergunta": pergunta_teste}, label2)
        
        # Comparacao
        print("\n" + "=" * 80)
        print("COMPARACAO DE RESULTADOS")
        print("=" * 80)
        print(f"\nVersao {label1}:")
        print(f"{resultado_v1[:200]}..." if len(resultado_v1) > 200 else resultado_v1)
        print(f"\n{'-' * 80}")
        print(f"\nVersao {label2}:")
        print(f"{resultado_v2[:200]}..." if len(resultado_v2) > 200 else resultado_v2)
        
    except Exception as e:
        print(f"\n[ERRO] Ao comparar versoes: {e}")


def testar_versao(prompt, variables: dict, version_tag: str):
    """
    Testa uma versao especifica do prompt com o LM Studio.
    
    Args:
        prompt: Objeto do prompt do Langfuse
        variables: Variaveis para compilar o prompt
        version_tag: Tag para identificar a versao no trace
    
    Returns:
        str: Resposta do modelo
    """
    try:
        # Compilar prompt
        prompt_text = prompt.compile(**variables)
        
        print(f"\nPrompt compilado ({version_tag}):")
        print(f"{prompt_text[:150]}...")
        
        # Fazer chamada para LM Studio
        response = client.chat.completions.create(
            model=os.getenv("LM_STUDIO_MODEL", "gemma-3-4b"),
            messages=[
                {"role": "user", "content": prompt_text}
            ],
            temperature=prompt.config.get("temperature", 0.7),
            max_tokens=prompt.config.get("max_tokens", 300)
        )
        
        output = response.choices[0].message.content
        
        print(f"[OK] Resposta recebida ({len(output)} caracteres)")
        print(f"     Tokens: {response.usage.total_tokens}")
        
        return output
        
    except Exception as e:
        print(f"[ERRO] Ao testar versao: {e}")
        return ""


def demonstracao_versionamento():
    """
    Demonstracao completa do fluxo de versionamento.
    """
    print("\n" + "=" * 80)
    print("DEMONSTRACAO: VERSIONAMENTO DE PROMPTS")
    print("=" * 80)
    
    # 1. Criar prompt inicial (v1)
    print("\n1. Criando versao inicial...")
    criar_prompt_inicial()
    
    # 2. Criar versao melhorada (v2)
    print("\n2. Criando versao melhorada (v2)...")
    criar_nova_versao(
        prompt_name="assistente_cliente",
        novo_texto="Voce e um assistente de atendimento ao cliente profissional e prestativo. Responda de forma clara, objetiva e sempre oferecendo solucoes: {{pergunta}}",
        nova_config={
            "model": "gemma-3-4b",
            "temperature": 0.5,  # Menos criativo, mais consistente
            "max_tokens": 400
        },
        tags=["v2", "melhorado", "profissional"]
    )
    
    # 3. Criar versao otimizada para eficiencia (v3)
    print("\n3. Criando versao otimizada (v3)...")
    criar_nova_versao(
        prompt_name="assistente_cliente",
        novo_texto="Assistente de atendimento: responda de forma direta e eficiente a pergunta: {{pergunta}}",
        nova_config={
            "model": "gemma-3-4b",
            "temperature": 0.3,  # Muito mais conservador
            "max_tokens": 250  # Respostas mais concisas
        },
        tags=["v3", "otimizado", "eficiente"]
    )
    
    # 4. Listar todas as versoes
    print("\n4. Listando todas as versoes criadas...")
    listar_versoes("assistente_cliente")
    
    # 5. Testar comparacao se LM Studio estiver disponivel
    print("\n5. Testando comparacao de versoes com LM Studio...")
    try:
        comparar_versoes(
            prompt_name="assistente_cliente",
            label1="v1",
            label2="v2",
            pergunta_teste="Como faco para cancelar meu pedido?"
        )
    except Exception as e:
        print(f"\n[AVISO] Nao foi possivel comparar versoes: {e}")
        print("        Certifique-se de que o LM Studio esta rodando")


def main():
    """
    Funcao principal.
    """
    print("\n" + "=" * 80)
    print("LANGFUSE - VERSIONAMENTO DE PROMPTS")
    print("Demonstracao com LM Studio (Gemma)")
    print("=" * 80)
    
    demonstracao_versionamento()
    
    # Flush para garantir que tudo foi enviado
    langfuse.flush()
    
    print("\n" + "=" * 80)
    print("SCRIPT FINALIZADO")
    print("=" * 80)
    print("\nDicas:")
    print("- Acesse http://localhost:3000 para ver o historico no Langfuse Dashboard")
    print("- Use versoes diferentes para A/B testing")
    print("- Sempre teste novas versoes antes de promover para producao")
    print("- Use labels (production, staging) para identificar o proposito de cada versao")


if __name__ == "__main__":
    main()
