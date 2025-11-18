"""
Script para transformar dados de Bronze para Silver
Bronze → Silver (Limpeza, Validação, Padronização e Extração NER)

Silver Layer Output:
- Dados individuais limpos
- Tipos padronizados
- NER labels extraídos do Label Studio
- Salvo em Parquet para próxima etapa
"""
import io
import re
import json
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from minio import Minio
from env_config import get_minio_config, get_labelstudio_config

def fetch_labelstudio_tasks(ls_url, ls_token, project_id):
    """
    Pega todas as tasks anotadas do Label Studio via API

    Args:
        ls_url: URL do Label Studio (ex: http://label-studio:8080)
        ls_token: Token de autenticação (access_token ou refresh_token)
        project_id: ID do projeto

    Returns:
        Lista de tasks com anotações, ou None se falhar
    """
    print(f"\n🔗 Conectando ao Label Studio: {ls_url}")
    print(f"   Projeto ID: {project_id}")

    url = f"{ls_url}/api/projects/{project_id}/tasks"

    # Tentar com Bearer (JWT token - para refresh_token ou access_token)
    try:
        headers = {
            'Authorization': f'Bearer {ls_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            tasks = response.json()
            print(f"   ✓ {len(tasks)} tasks recuperadas com Bearer token")
            return format_tasks(tasks)
        elif response.status_code == 401:
            print(f"   ⚠️  Bearer token falhou (401). Tentando com Token scheme...")
    except Exception as e:
        print(f"   ⚠️  Erro com Bearer: {str(e)}")

    # Tentar com Token scheme (DRF token auth)
    try:
        headers = {
            'Authorization': f'Token {ls_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            tasks = response.json()
            print(f"   ✓ {len(tasks)} tasks recuperadas com Token scheme")
            return format_tasks(tasks)
        elif response.status_code == 401:
            print(f"   ⚠️  Token scheme falhou (401). Tentando sem autenticação...")
    except Exception as e:
        print(f"   ⚠️  Erro com Token: {str(e)}")

    # Última tentativa: sem autenticação (se Label Studio está em modo público)
    try:
        response = requests.get(url)

        if response.status_code == 200:
            tasks = response.json()
            print(f"   ✓ {len(tasks)} tasks recuperadas (sem autenticação)")
            return format_tasks(tasks)
    except Exception as e:
        print(f"   ⚠️  Erro sem autenticação: {str(e)}")

    print(f"\n   ❌ Nenhum método de autenticação funcionou")
    print(f"   ❌ Verifique:")
    print(f"      1. URL do Label Studio está correto")
    print(f"      2. Token é válido e não expirou")
    print(f"      3. Projeto ID {project_id} existe")
    return None


def format_tasks(tasks):
    """Converte formato da API Label Studio para formato compatível"""
    formatted_tasks = []
    for task in tasks:
        formatted_task = {
            'id': task.get('id'),
            'data': task.get('data', {}),
            'annotations': task.get('annotations', [])  # Manter como lista estruturada
        }
        formatted_tasks.append(formatted_task)
    return formatted_tasks

def extract_ner_labels(annotations_list, debug=False):
    """
    Extrai labels NER do formato estruturado do Label Studio

    Args:
        annotations_list: Lista de anotações estruturadas da API
        debug: Se True, exibe logs detalhados da extração

    Returns:
        dict: Labels extraídos {field_name: value}
    """
    if not annotations_list or not isinstance(annotations_list, list):
        if debug:
            print(f"   [DEBUG] annotations_list vazio ou não é list: {type(annotations_list)}")
        return {}

    labels_dict = {}

    try:
        if debug:
            print(f"   [DEBUG] Processando {len(annotations_list)} anotação(ões)")

        # Iterar sobre anotações (pode haver múltiplas anotações por task)
        for ann_idx, annotation in enumerate(annotations_list):
            if not isinstance(annotation, dict):
                if debug:
                    print(f"      [DEBUG] Anotação {ann_idx} não é dict: {type(annotation)}")
                continue

            results = annotation.get('result', [])
            if not results:
                if debug:
                    print(f"      [DEBUG] Anotação {ann_idx} sem 'result' ou result vazio")
                continue

            if debug:
                print(f"      [DEBUG] Anotação {ann_idx} tem {len(results)} resultado(s)")

            # Iterar sobre resultados (entidades extraídas)
            for res_idx, result in enumerate(results):
                if not isinstance(result, dict):
                    if debug:
                        print(f"         [DEBUG] Resultado {res_idx} não é dict: {type(result)}")
                    continue

                # Label Studio estrutura: result.value.labels[0] = nome do label
                value = result.get('value', {})
                if not isinstance(value, dict):
                    if debug:
                        print(f"         [DEBUG] Resultado {res_idx} 'value' não é dict: {type(value)}")
                    continue

                labels = value.get('labels', [])
                if not labels or not isinstance(labels, list):
                    if debug:
                        print(f"         [DEBUG] Resultado {res_idx} sem 'labels' ou não é list: {labels}")
                    continue

                label_name = labels[0].lower().strip()  # Nome do label (cliente, produto, etc)
                text = value.get('text', '').strip()    # Texto extraído

                if not text:
                    if debug:
                        print(f"         [DEBUG] Label '{label_name}' tem text vazio")
                    continue

                if debug:
                    print(f"         [EXTRAÍDO] {label_name}: '{text}'")

                # Padronizar tipos de dados específicos
                if label_name == 'quantidade':
                    try:
                        # Remover 'unidade(s)' ou similar
                        text_clean = re.sub(r'\s*(unidade|un|pc|pç|pcs|peça|peças)\(s?\)?', '', text, flags=re.IGNORECASE)
                        labels_dict[label_name] = int(float(text_clean))
                    except (ValueError, AttributeError):
                        labels_dict[label_name] = text

                elif label_name == 'avaliacao':
                    # Mapear avaliação qualitativa para numérica
                    avaliacao_map = {
                        'péssima': 1,
                        'ruim': 2,
                        'neutra': 3,
                        'boa': 4,
                        'ótima': 5
                    }
                    text_lower = text.lower().strip()
                    if text_lower in avaliacao_map:
                        labels_dict[label_name] = avaliacao_map[text_lower]
                    else:
                        # Se for número, tentar converter
                        try:
                            labels_dict[label_name] = int(float(text))
                        except (ValueError, AttributeError):
                            labels_dict[label_name] = text

                elif label_name == 'valor':
                    try:
                        # Limpar formato monetário (R$ 123,45 → 123.45)
                        text_clean = re.sub(r'[^\d,.-]', '', text)
                        labels_dict[label_name] = float(text_clean.replace(',', '.'))
                    except (ValueError, AttributeError):
                        labels_dict[label_name] = text

                else:
                    # String fields - manter primeiro valor encontrado para cada label
                    if label_name not in labels_dict:
                        labels_dict[label_name] = text.lower().strip()

    except Exception as e:
        # Log de erro para debug
        import traceback
        print(f"[WARN] Erro ao extrair NER labels: {str(e)}")
        traceback.print_exc()

    return labels_dict

def run():
    """Executa transformação Silver"""
    # Carregamento seguro de configurações (sem valores padrão para credenciais)
    minio_config = get_minio_config()
    labelstudio_config = get_labelstudio_config()

    MINIO_ENDPOINT = minio_config["endpoint"]
    MINIO_ACCESS_KEY = minio_config["access_key"]
    MINIO_SECRET_KEY = minio_config["secret_key"]

    # Label Studio API
    LABELSTUDIO_URL = labelstudio_config["url"]
    LABELSTUDIO_TOKEN = labelstudio_config["token"]
    LABELSTUDIO_PROJECT = labelstudio_config["project_id"]

    print("=" * 70)
    print("🧹 SILVER LAYER: Limpeza, Validação e Padronização")
    print("=" * 70)

    # Inicializar cliente MinIO uma vez (usado tanto para leitura quanto escrita)
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    try:
        # 1. Tentar puxar dados do Label Studio API
        print("\n📖 Tentando puxar dados do Label Studio API...")

        # PRIORIDADE 1: Ler dados do MinIO Bronze (contém TODOS os 1000+ registros do Label Studio)
        print("\n📂 Lendo dados do MinIO Bronze...")
        raw_data = None
        try:
            objects = list(client.list_objects("bronze", recursive=True))
            json_files = [o for o in objects if o.object_name.endswith('.json')]

            if json_files:
                latest_bronze = max(json_files, key=lambda x: x.last_modified)
                print(f"   ✓ Arquivo mais recente: {latest_bronze.object_name}")

                response = client.get_object("bronze", latest_bronze.object_name)
                raw_json = json.loads(response.read().decode('utf-8'))

                # Converter formato MinIO para formato compatível
                raw_data = []
                if isinstance(raw_json, list):
                    raw_data = raw_json
                elif isinstance(raw_json, dict) and 'data' in raw_json:
                    raw_data = raw_json['data']

                print(f"   ✓ {len(raw_data)} registros carregados do Bronze")
            else:
                print(f"   ⚠️  Nenhum arquivo JSON em Bronze, tentando API...")
        except Exception as e:
            print(f"   ⚠️  Erro ao ler Bronze: {str(e)}, tentando API...")

        # PRIORIDADE 2: Se Bronze vazio/falhou, conectar à API do Label Studio como fallback
        if not raw_data:
            print("\n🔗 Conectando à API do Label Studio como fallback...")
            if LABELSTUDIO_TOKEN:
                raw_data = fetch_labelstudio_tasks(
                    LABELSTUDIO_URL,
                    LABELSTUDIO_TOKEN,
                    LABELSTUDIO_PROJECT
                )
            else:
                print(f"   ❌ Sem credenciais de API")

        if not raw_data:
            raise ValueError("❌ Nenhuma fonte de dados disponível (Bronze e API falharam)")

        print(f"   ✓ Total de registros: {len(raw_data)}")

        # 2. Processar e limpar dados
        print("\n🔍 Validando e limpando dados...")
        cleaned_records = []
        stats = {
            'invalid_id': 0,
            'invalid_data': 0,
            'total_removed': 0
        }

        for record in raw_data:
            is_valid = True

            # Validação: ID obrigatório
            if not record.get('id'):
                stats['invalid_id'] += 1
                is_valid = False

            # Validação: Data com conteúdo
            if not record.get('data') or (isinstance(record.get('data'), dict) and not record['data']):
                stats['invalid_data'] += 1
                is_valid = False

            if is_valid:
                cleaned_records.append(record)
            else:
                stats['total_removed'] += 1

        print(f"   ✓ {len(cleaned_records)} registros válidos")
        print(f"   ✗ {stats['total_removed']} registros removidos")

        # 3. Extrair NER labels e padronizar
        print("\n🏷️  Extraindo labels NER e padronizando...")
        standardized_records = []
        ner_stats = {
            'with_ner': 0,
            'without_ner': 0,
            'with_valor': 0,
            'without_valor': 0,
            'with_cliente': 0,
            'with_produto': 0
        }

        for idx, record in enumerate(cleaned_records):
            # Extrair labels (annotations é uma lista estruturada, não uma string)
            # Ativar debug para primeiros 3 registros
            debug_mode = idx < 3
            ner_data = extract_ner_labels(record.get('annotations', []), debug=debug_mode)

            # Contar registros com/sem dados NER
            if ner_data:
                ner_stats['with_ner'] += 1
            else:
                ner_stats['without_ner'] += 1

            # Estatísticas detalhadas por campo
            if ner_data.get('valor'):
                ner_stats['with_valor'] += 1
            else:
                ner_stats['without_valor'] += 1

            if ner_data.get('cliente'):
                ner_stats['with_cliente'] += 1

            if ner_data.get('produto'):
                ner_stats['with_produto'] += 1

            # Criar registro padronizado
            standardized = {
                'id': record.get('id'),
                'labelstudio_id': record.get('id'),
                'data_bruta': record.get('data'),
                'annotations_raw': '',  # Não salvar annotations brutas em Parquet (complexo demais)
                # NER extracted fields
                'cliente': ner_data.get('cliente'),
                'produto': ner_data.get('produto'),
                'quantidade': ner_data.get('quantidade'),
                'valor': ner_data.get('valor'),
                'canal': ner_data.get('canal'),
                'forma_pagamento': ner_data.get('forma_pagamento'),
                'status': ner_data.get('status'),
                'cidade': ner_data.get('cidade'),
                'avaliacao': ner_data.get('avaliacao'),
                'data': ner_data.get('data'),
                'sentimento': ner_data.get('sentimento'),
                # Metadata
                'timestamp_processamento': datetime.now().isoformat()
            }
            standardized_records.append(standardized)

        print(f"   ✓ {ner_stats['with_ner']} registros com algum dados NER extraído")
        print(f"   ⚠️  {ner_stats['without_ner']} registros SEM dados NER (campos vazios)")

        # 4. Converter para DataFrame
        print("\n📊 Convertendo para DataFrame...")
        df = pd.DataFrame(standardized_records)
        print(f"   ✓ Shape: {df.shape}")
        print(f"   ✓ Colunas: {list(df.columns)}")

        # 4.1 Dados vêm apenas do Label Studio (sem preenchimento sintético)
        print("\n📊 Resumo de dados extraídos da API do Label Studio:")
        print(f"\n   🏷️  Estatísticas de Extração NER:")
        print(f"      • Registros com algum NER: {ner_stats['with_ner']}")
        print(f"      • Registros sem nenhum NER: {ner_stats['without_ner']}")
        print(f"      • Registros com 'valor': {ner_stats['with_valor']}")
        print(f"      • Registros SEM 'valor': {ner_stats['without_valor']}")
        print(f"      • Registros com 'cliente': {ner_stats['with_cliente']}")
        print(f"      • Registros com 'produto': {ner_stats['with_produto']}")

        print(f"\n   Campos com dados (não-nulos):")
        for col in df.columns:
            non_null = df[col].notna().sum()
            if non_null > 0:
                print(f"      • {col}: {non_null} registros")

        print(f"\n   Campos vazios (nulos):")
        for col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                print(f"      • {col}: {null_count} registros vazios")

        # 5. Preparar dados para Parquet (remover colunas com tipos complexos)
        print("\n🧹 Limpando tipos de dados para Parquet...")

        # Remover colunas que causam problemas de serialização
        columns_to_drop = ['data_bruta', 'annotations_raw']
        df_parquet = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

        # Manter texto columns como None/NaN para que notna() funcione corretamente downstream
        # Isto é crítico para aggregate_gold.py que filtra baseado em notna()
        text_columns = ['cliente', 'produto', 'canal', 'forma_pagamento', 'status', 'cidade', 'sentimento', 'data']
        for col in text_columns:
            if col in df_parquet.columns:
                # Converter para string apenas se não for None
                df_parquet[col] = df_parquet[col].apply(lambda x: str(x).lower().strip() if pd.notna(x) and str(x).strip() != '' else None)

        # Converter numeric columns
        numeric_columns = ['quantidade', 'valor', 'avaliacao']
        for col in numeric_columns:
            if col in df_parquet.columns:
                df_parquet[col] = pd.to_numeric(df_parquet[col], errors='coerce')

        print(f"   ✓ Colunas finais: {list(df_parquet.columns)}")

        # 6. Salvar em Parquet na Silver
        print("\n💾 Salvando em Parquet...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"silver_clean_{timestamp}.parquet"

        parquet_buffer = io.BytesIO()
        df_parquet.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
        parquet_buffer.seek(0)

        client.put_object(
            "silver",
            filename,
            parquet_buffer,
            parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        print(f"   ✓ {filename} salvo em Silver!")

        # 7. Resumo
        print("\n" + "=" * 70)
        print("✅ RESUMO DA TRANSFORMAÇÃO SILVER")
        print("=" * 70)
        print(f"   Registros válidos:  {len(cleaned_records)}")
        print(f"   Registros removidos: {stats['total_removed']}")
        print(f"   Taxa de limpeza:    {(stats['total_removed'] / len(raw_data) * 100):.1f}%")
        print(f"   Colunas Parquet:    {len(df_parquet.columns)}")
        print(f"   Arquivo Parquet:    {filename}")
        print(f"   Tamanho:            {df_parquet.memory_usage(deep=True).sum() / 1024:.2f} KB")
        print("=" * 70)

        return {
            'status': 'success',
            'records_valid': len(cleaned_records),
            'records_removed': int(stats['total_removed']),
            'filename': filename,
            'total_input': len(raw_data)
        }

    except Exception as e:
        print(f"\n❌ Erro: {str(e)}")
        raise

if __name__ == "__main__":
    run()
