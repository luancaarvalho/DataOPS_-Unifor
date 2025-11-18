"""
Script para consolidar e agregar dados na Gold
Silver Parquet → Gold Parquet (Dados agregados e prontos para análise)

Gold Layer Output:
- Dados consolidados por dimensão (Cliente, Produto, Canal, etc)
- Métricas e KPIs pré-calculados
- Pronto para visualização no Streamlit (sem processamento adicional)
"""
import json
import io
from datetime import datetime
from minio import Minio
import pandas as pd
import numpy as np
from env_config import get_minio_config

def run():
    """Executa agregação e consolidação Gold"""
    # Carregamento seguro de configurações (sem valores padrão para credenciais)
    minio_config = get_minio_config()

    MINIO_ENDPOINT = minio_config["endpoint"]
    MINIO_ACCESS_KEY = minio_config["access_key"]
    MINIO_SECRET_KEY = minio_config["secret_key"]

    print("=" * 70)
    print("📊 GOLD LAYER: Consolidação e Agregação Analítica")
    print("=" * 70)

    try:
        # 1. Ler Silver Parquet
        print("\n📖 Lendo dados da Silver...")
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        objects = list(client.list_objects("silver", recursive=True))
        if not objects:
            raise ValueError("❌ Nenhum arquivo em Silver!")

        parquet_files = [o for o in objects if o.object_name.endswith('.parquet')]
        if not parquet_files:
            raise ValueError("❌ Nenhum arquivo Parquet em Silver!")

        latest_silver = max(parquet_files, key=lambda x: x.last_modified)
        print(f"   ✓ Arquivo: {latest_silver.object_name}")

        response = client.get_object("silver", latest_silver.object_name)
        silver_df = pd.read_parquet(io.BytesIO(response.read()))
        print(f"   ✓ {len(silver_df)} registros carregados")
        print(f"   ✓ Colunas: {list(silver_df.columns)}")

        # Diagnóstico: verificar dados disponíveis
        print(f"\n📊 Diagnóstico de dados em Silver:")
        for col in silver_df.columns:
            non_null = silver_df[col].notna().sum()
            if non_null > 0:
                print(f"   ✓ {col}: {non_null} registros com dados")

        # 2. Preparar dados para agregação (SEM descartar registros vazios)
        print("\n🧹 Preparando dados para agregação...")

        # NÃO remover registros - apenas padronizar tipos
        df_clean = silver_df.copy()

        # Remover espaços em branco e converter vazios em NaN
        for col in ['cliente', 'produto', 'canal', 'cidade', 'forma_pagamento', 'status', 'sentimento', 'data']:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace(['', 'nan', 'None'], None)

        # Garantir tipos de dados corretos
        df_clean['valor'] = pd.to_numeric(df_clean['valor'], errors='coerce')
        df_clean['quantidade'] = pd.to_numeric(df_clean['quantidade'], errors='coerce').fillna(1).astype(int)
        df_clean['avaliacao'] = pd.to_numeric(df_clean['avaliacao'], errors='coerce')

        # Remover apenas registros completamente vazios
        # Filtro: manter registros que têm VALOR (obrigatório para agregações!)

        # Debug: quantos registros têm valor
        has_valor = df_clean['valor'].notna().sum()
        print(f"\n   Debug: {has_valor} registros com 'valor' preenchido")

        if has_valor == 0:
            print("   ❌ ERRO: Nenhum registro com 'valor' encontrado!")
            print("   Verifique se a extração de NER está funcionando")
            raise ValueError("Nenhum registro com 'valor' - NER não extraiu dados!")

        # Manter registros que têm VALOR
        df_clean = df_clean[df_clean['valor'].notna()].copy()

        print(f"   ✓ {len(df_clean)} registros para agregação")

        # 3. Criar agregações por dimensão
        print("\n📊 Gerando agregações...")

        # ========== AGREGAÇÃO 1: POR CLIENTE ==========
        df_cliente = df_clean.groupby('cliente').agg({
            'valor': ['sum', 'mean', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_cliente.columns = ['cliente', 'valor_total', 'valor_medio', 'qtd_transacoes', 'quantidade_vendida']
        df_cliente = df_cliente.sort_values('valor_total', ascending=False)
        print(f"   ✓ {len(df_cliente)} clientes agregados")

        # ========== AGREGAÇÃO 2: POR PRODUTO ==========
        df_produto = df_clean.groupby('produto').agg({
            'valor': ['sum', 'mean', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_produto.columns = ['produto', 'valor_total', 'valor_medio', 'qtd_transacoes', 'quantidade_vendida']
        df_produto = df_produto.sort_values('valor_total', ascending=False)
        print(f"   ✓ {len(df_produto)} produtos agregados")

        # ========== AGREGAÇÃO 3: POR CANAL ==========
        df_canal = df_clean.groupby('canal').agg({
            'valor': ['sum', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_canal.columns = ['canal', 'valor_total', 'qtd_transacoes', 'quantidade_vendida']
        df_canal = df_canal.sort_values('valor_total', ascending=False)
        print(f"   ✓ {len(df_canal)} canais agregados")

        # ========== AGREGAÇÃO 4: POR STATUS ==========
        df_status = df_clean.groupby('status').agg({
            'valor': ['sum', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_status.columns = ['status', 'valor_total', 'qtd_transacoes', 'quantidade_vendida']
        df_status = df_status.sort_values('qtd_transacoes', ascending=False)
        print(f"   ✓ {len(df_status)} status agregados")

        # ========== AGREGAÇÃO 5: POR CIDADE ==========
        df_cidade = df_clean.dropna(subset=['cidade']).groupby('cidade').agg({
            'valor': ['sum', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_cidade.columns = ['cidade', 'valor_total', 'qtd_transacoes', 'quantidade_vendida']
        df_cidade = df_cidade.sort_values('valor_total', ascending=False)
        print(f"   ✓ {len(df_cidade)} cidades agregadas")

        # ========== AGREGAÇÃO 6: POR FORMA DE PAGAMENTO ==========
        df_pagamento = df_clean.dropna(subset=['forma_pagamento']).groupby('forma_pagamento').agg({
            'valor': ['sum', 'mean', 'count'],
            'quantidade': 'sum'
        }).round(2).reset_index()
        df_pagamento.columns = ['forma_pagamento', 'valor_total', 'valor_medio', 'qtd_transacoes', 'quantidade_vendida']
        df_pagamento = df_pagamento.sort_values('qtd_transacoes', ascending=False)
        print(f"   ✓ {len(df_pagamento)} formas de pagamento agregadas")

        # ========== KPIs GLOBAIS ==========
        kpis = {
            'total_vendas': float(df_clean['valor'].sum()),
            'quantidade_total': int(df_clean['quantidade'].sum()),
            'qtd_clientes': len(df_clean['cliente'].unique()),
            'qtd_produtos': len(df_clean['produto'].unique()),
            'qtd_canais': len(df_clean['canal'].unique()),
            'qtd_transacoes': len(df_clean),
            'valor_medio_transacao': float(df_clean['valor'].mean()),
            'ticket_medio': float(df_clean.groupby('cliente')['valor'].sum().mean()),
        }
        print(f"   ✓ KPIs calculados: {len(kpis)} métricas")

        # 4. Consolidar tudo em um único DataFrame ANALÍTICO
        print("\n🔗 Consolidando dados analíticos...")

        # Verificar se há registros para processar
        if len(df_clean) == 0:
            print("   ❌ ERRO: df_clean está vazio!")
            print("   Nenhum registro com 'valor' após filtro")
            raise ValueError("Nenhum registro para consolidação - DataFrame vazio!")

        # Criar tabela de dimensões consolidadas
        consolidated = []

        for idx, row in df_clean.iterrows():
            # Usar try/except para lidar com valores None
            try:
                valor = float(row['valor']) if pd.notna(row['valor']) else 0.0
                quantidade = int(row['quantidade']) if pd.notna(row['quantidade']) else 1
            except (ValueError, TypeError):
                valor = 0.0
                quantidade = 1

            record = {
                # Dados originais
                'id': row['id'] if 'id' in row and pd.notna(row['id']) else None,
                'data': row['data'] if 'data' in row and pd.notna(row['data']) else None,
                'cliente': row['cliente'] if 'cliente' in row and pd.notna(row['cliente']) else None,
                'produto': row['produto'] if 'produto' in row and pd.notna(row['produto']) else None,
                'quantidade': quantidade,
                'valor': valor,
                'canal': row['canal'] if 'canal' in row and pd.notna(row['canal']) else None,
                'forma_pagamento': row['forma_pagamento'] if 'forma_pagamento' in row and pd.notna(row['forma_pagamento']) else None,
                'status': row['status'] if 'status' in row and pd.notna(row['status']) else None,
                'cidade': row['cidade'] if 'cidade' in row and pd.notna(row['cidade']) else None,
                'avaliacao': row['avaliacao'] if 'avaliacao' in row and pd.notna(row['avaliacao']) else None,
                'sentimento': row['sentimento'] if 'sentimento' in row and pd.notna(row['sentimento']) else None,
                # Cálculos derivados para análise
                'receita': valor * quantidade,
                'valor_unitario': valor / quantidade if quantidade > 0 else 0.0,
            }
            consolidated.append(record)

        if len(consolidated) == 0:
            print("   ❌ ERRO: consolidated está vazio!")
            raise ValueError("Nenhum registro consolidado!")

        df_analytics = pd.DataFrame(consolidated)
        print(f"   ✓ Tabela consolidada: {len(df_analytics)} registros")

        # 5. Salvar Parquets em Gold
        print("\n💾 Salvando em Gold...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        files_saved = []

        # Arquivo principal: Dados consolidados
        analytics_file = f"gold_analytics_{timestamp}.parquet"
        buf = io.BytesIO()
        df_analytics.to_parquet(buf, engine='pyarrow', compression='snappy', index=False)
        buf.seek(0)
        client.put_object("gold", analytics_file, buf, buf.getbuffer().nbytes)
        files_saved.append(analytics_file)
        print(f"   ✓ {analytics_file}")

        # Agregações por dimensão
        dimensions = [
            ('gold_dim_cliente', df_cliente),
            ('gold_dim_produto', df_produto),
            ('gold_dim_canal', df_canal),
            ('gold_dim_status', df_status),
            ('gold_dim_cidade', df_cidade),
            ('gold_dim_pagamento', df_pagamento),
        ]

        for dim_name, dim_df in dimensions:
            dim_file = f"{dim_name}_{timestamp}.parquet"
            buf = io.BytesIO()
            dim_df.to_parquet(buf, engine='pyarrow', compression='snappy', index=False)
            buf.seek(0)
            client.put_object("gold", dim_file, buf, buf.getbuffer().nbytes)
            files_saved.append(dim_file)
            print(f"   ✓ {dim_file}")

        # Metadados JSON
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'layer': 'gold',
            'source': latest_silver.object_name,
            'kpis': kpis,
            'aggregations': {
                'clientes': len(df_cliente),
                'produtos': len(df_produto),
                'canais': len(df_canal),
                'status': len(df_status),
                'cidades': len(df_cidade),
                'formas_pagamento': len(df_pagamento),
            },
            'files_generated': files_saved
        }

        meta_file = f"gold_metadata_{timestamp}.json"
        meta_json = json.dumps(metadata, ensure_ascii=False, indent=2).encode('utf-8')
        client.put_object("gold", meta_file, io.BytesIO(meta_json), len(meta_json))
        print(f"   ✓ {meta_file}")

        # 6. Resumo
        print("\n" + "=" * 70)
        print("✅ RESUMO DA CONSOLIDAÇÃO GOLD")
        print("=" * 70)
        print(f"   Registros analíticos:    {len(df_analytics)}")
        print(f"   Total de vendas (R$):    {kpis['total_vendas']:,.2f}")
        print(f"   Quantidade vendida:      {kpis['quantidade_total']:,.0f}")
        print(f"   Clientes únicos:         {kpis['qtd_clientes']}")
        print(f"   Produtos:                {kpis['qtd_produtos']}")
        print(f"   Canais:                  {kpis['qtd_canais']}")
        print(f"   Valor médio transação:   R$ {kpis['valor_medio_transacao']:,.2f}")
        print(f"   Ticket médio cliente:    R$ {kpis['ticket_medio']:,.2f}")
        print(f"   Arquivos gerados:        {len(files_saved) + 1}")
        print("=" * 70)

        return {
            'status': 'success',
            'records': len(df_analytics),
            'filename': analytics_file,
            'kpis': kpis
        }

    except Exception as e:
        print(f"\n❌ Erro: {str(e)}")
        raise

if __name__ == "__main__":
    run()
