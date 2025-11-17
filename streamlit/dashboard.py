"""
Streamlit Dashboard - Varejo Inteligente
Lê dados já agregados do Gold e apenas visualiza
Sem processamento adicional - dados prontos para análise
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from minio import Minio
import io
import json
from env_config import get_minio_config

st.set_page_config(
    page_title="Varejo Inteligente - Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS
st.markdown("""
    <style>
    .title-main { color: #0066cc; font-size: 2.5em; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# Carregamento seguro de configurações (sem valores padrão para credenciais)
minio_cfg = get_minio_config()
MINIO_CONFIG = {
    'endpoint': minio_cfg['endpoint'],
    'access_key': minio_cfg['access_key'],
    'secret_key': minio_cfg['secret_key'],
    'secure': False
}

@st.cache_data(ttl=60)
def get_analytics():
    """Lê gold_analytics_*.parquet"""
    try:
        client = Minio(**MINIO_CONFIG)
        objects = list(client.list_objects('gold', recursive=True))
        files = [o for o in objects if 'gold_analytics' in o.object_name and '.parquet' in o.object_name]

        if not files:
            return None, "Nenhum arquivo encontrado"

        latest = max(files, key=lambda x: x.last_modified)
        response = client.get_object('gold', latest.object_name)
        df = pd.read_parquet(io.BytesIO(response.read()))

        return df, latest.object_name
    except Exception as e:
        return None, str(e)

@st.cache_data(ttl=60)
def get_dimension(dim_name):
    """Lê gold_dim_*.parquet"""
    try:
        client = Minio(**MINIO_CONFIG)
        objects = list(client.list_objects('gold', recursive=True))
        files = [o for o in objects if f'gold_dim_{dim_name}' in o.object_name and '.parquet' in o.object_name]

        if not files:
            return None

        latest = max(files, key=lambda x: x.last_modified)
        response = client.get_object('gold', latest.object_name)
        return pd.read_parquet(io.BytesIO(response.read()))
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_pipeline_stats():
    """Obtém estatísticas do pipeline (registros por camada)"""
    try:
        client = Minio(**MINIO_CONFIG)

        stats = {
            'bronze': 0,
            'silver': 0,
            'gold': 0,
            'bronze_timestamp': 'N/A',
            'silver_timestamp': 'N/A',
            'gold_timestamp': 'N/A'
        }

        # Contar registros em Bronze (JSON)
        try:
            bronze_files = list(client.list_objects('bronze', recursive=True))
            bronze_json = [f for f in bronze_files if f.object_name.endswith('.json')]
            if bronze_json:
                latest_bronze = max(bronze_json, key=lambda x: x.last_modified)
                response = client.get_object('bronze', latest_bronze.object_name)
                data = json.loads(response.read().decode('utf-8'))
                stats['bronze'] = len(data) if isinstance(data, list) else (len(data.get('data', [])) if isinstance(data, dict) else 0)
                stats['bronze_timestamp'] = latest_bronze.last_modified.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

        # Contar registros em Silver (Parquet)
        try:
            silver_files = list(client.list_objects('silver', recursive=True))
            silver_parquet = [f for f in silver_files if f.object_name.endswith('.parquet')]
            if silver_parquet:
                latest_silver = max(silver_parquet, key=lambda x: x.last_modified)
                response = client.get_object('silver', latest_silver.object_name)
                df_silver = pd.read_parquet(io.BytesIO(response.read()))
                stats['silver'] = len(df_silver)
                stats['silver_timestamp'] = latest_silver.last_modified.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

        # Contar registros em Gold (Parquet)
        try:
            gold_files = list(client.list_objects('gold', recursive=True))
            gold_parquet = [f for f in gold_files if 'gold_analytics' in f.object_name and f.object_name.endswith('.parquet')]
            if gold_parquet:
                latest_gold = max(gold_parquet, key=lambda x: x.last_modified)
                response = client.get_object('gold', latest_gold.object_name)
                df_gold = pd.read_parquet(io.BytesIO(response.read()))
                stats['gold'] = len(df_gold)
                stats['gold_timestamp'] = latest_gold.last_modified.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

        return stats
    except Exception as e:
        return None

# ============================================
# HEADER
# ============================================
st.markdown('<div class="title-main">🛒 VAREJO INTELIGENTE</div>', unsafe_allow_html=True)
st.markdown("Análise de Vendas em Tempo Real")
st.markdown("---")

gold_df, gold_info = get_analytics()

if gold_df is None:
    st.error(f"❌ {gold_info}")
    st.stop()

st.caption(f"📄 Arquivo: {gold_info}")

# ============================================
# KPIs PRINCIPAIS
# ============================================
st.subheader("📊 Principais Indicadores")

# Função para formatar valores grandes
def format_currency(value):
    """Formata valores grandes em notação compacta"""
    if value >= 1_000_000:
        return f"R$ {value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"R$ {value / 1_000:.0f}K"
    else:
        return f"R$ {value:.2f}"

col1, col2, col3, col4, col5 = st.columns(5)

total_vendas = gold_df['receita'].sum() if 'receita' in gold_df.columns else gold_df['valor'].sum()
qtd_total = gold_df['quantidade'].sum() if 'quantidade' in gold_df.columns else 0
num_clientes = gold_df['cliente'].nunique()
num_produtos = gold_df['produto'].nunique()
ticket_medio = total_vendas / len(gold_df) if len(gold_df) > 0 else 0

with col1:
    st.metric("💰 Total Vendas", format_currency(total_vendas))
with col2:
    st.metric("📦 Quantidade", f"{qtd_total:,.0f}")
with col3:
    st.metric("👥 Clientes", f"{num_clientes:,}")
with col4:
    st.metric("🏷️ Produtos", f"{num_produtos:,}")
with col5:
    st.metric("💳 Ticket Médio", format_currency(ticket_medio))

st.markdown("---")

# ============================================
# ANÁLISES POR ABAS
# ============================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Vendas",
    "👥 Clientes",
    "🏷️ Produtos",
    "🏙️ Geográfico",
    "💳 Pagamento",
    "📊 Dados Brutos",
    "🔧 Pipeline"
])

# TAB 1: VENDAS
with tab1:
    st.subheader("📈 Análise de Vendas")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Distribuição por Canal")
        df_canal = get_dimension('canal')
        if df_canal is not None and len(df_canal) > 0:
            fig = px.pie(df_canal, values='valor_total', names='canal', title="Vendas por Canal")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de canal")

    with col2:
        st.markdown("#### Status dos Pedidos")
        df_status = get_dimension('status')
        if df_status is not None and len(df_status) > 0:
            fig = px.bar(df_status, x='status', y='qtd_transacoes', title="Pedidos por Status")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de status")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top 10 Maiores Vendas")
        top10 = gold_df.nlargest(10, 'valor')[['cliente', 'produto', 'valor', 'data']]
        st.dataframe(top10, use_container_width=True)

    with col2:
        st.markdown("#### Distribuição de Valores")
        if len(gold_df) > 0:
            fig = px.histogram(gold_df, x='valor', nbins=50, title="Histograma de Valores")
            st.plotly_chart(fig, use_container_width=True)

# TAB 2: CLIENTES
with tab2:
    st.subheader("👥 Análise de Clientes")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top 10 Clientes (Valor)")
        df_cliente = get_dimension('cliente')
        if df_cliente is not None and len(df_cliente) > 0:
            df_top = df_cliente.head(10)
            fig = px.bar(df_top.sort_values('valor_total'), y='cliente', x='valor_total', orientation='h', title="Top Clientes por Valor")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Top 10 Clientes (Quantidade)")
        if df_cliente is not None and len(df_cliente) > 0:
            df_top = df_cliente.head(10)
            fig = px.bar(df_top.sort_values('quantidade_vendida'), y='cliente', x='quantidade_vendida', orientation='h', title="Top Clientes por Qtd")
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Clientes por Avaliação")
        if 'avaliacao' in gold_df.columns:
            aval_data = gold_df['avaliacao'].value_counts().sort_index()
            if len(aval_data) > 0:
                fig = px.bar(x=aval_data.index, y=aval_data.values, title="Distribuição de Avaliações")
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Análise de Sentimento")
        if 'sentimento' in gold_df.columns:
            sent_data = gold_df['sentimento'].value_counts()
            if len(sent_data) > 0:
                fig = px.pie(values=sent_data.values, names=sent_data.index, title="Sentimento dos Clientes")
                st.plotly_chart(fig, use_container_width=True)

# TAB 3: PRODUTOS
with tab3:
    st.subheader("🏷️ Análise de Produtos")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top Produtos (Quantidade)")
        df_produto = get_dimension('produto')
        if df_produto is not None and len(df_produto) > 0:
            df_top = df_produto.head(10)
            fig = px.bar(df_top.sort_values('quantidade_vendida'), y='produto', x='quantidade_vendida', orientation='h', title="Top Produtos")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Top Produtos (Receita)")
        if df_produto is not None and len(df_produto) > 0:
            df_top = df_produto.head(10)
            fig = px.bar(df_top.sort_values('valor_total'), y='produto', x='valor_total', orientation='h', title="Receita por Produto")
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Produtos Mais Vendidos")
        if df_produto is not None:
            st.dataframe(df_produto[['produto', 'quantidade_vendida', 'valor_total']].head(15), use_container_width=True)

    with col2:
        st.markdown("#### Correlação: Quantidade vs Valor")
        if len(gold_df) > 0:
            fig = px.scatter(gold_df, x='quantidade', y='valor', title="Quantidade x Valor", opacity=0.6)
            st.plotly_chart(fig, use_container_width=True)

# TAB 4: GEOGRÁFICO
with tab4:
    st.subheader("🏙️ Análise Geográfica")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Vendas por Cidade")
        df_cidade = get_dimension('cidade')
        if df_cidade is not None and len(df_cidade) > 0:
            df_top = df_cidade.head(15)
            fig = px.bar(df_top.sort_values('valor_total'), y='cidade', x='valor_total', orientation='h', title="Top 15 Cidades por Vendas")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Quantidade por Cidade")
        if df_cidade is not None and len(df_cidade) > 0:
            df_top = df_cidade.head(15)
            fig = px.bar(df_top.sort_values('quantidade_vendida'), y='cidade', x='quantidade_vendida', orientation='h', title="Top 15 Cidades por Qtd")
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Distribuição de Cidades")
    if df_cidade is not None and len(df_cidade) > 0:
        fig = px.pie(df_cidade, values='valor_total', names='cidade', title="Proporção por Cidade")
        st.plotly_chart(fig, use_container_width=True)

# TAB 5: PAGAMENTO
with tab5:
    st.subheader("💳 Análise de Pagamento")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Formas de Pagamento")
        df_pagto = get_dimension('pagamento')
        if df_pagto is not None and len(df_pagto) > 0:
            fig = px.pie(df_pagto, values='qtd_transacoes', names='forma_pagamento', title="Distribuição de Formas")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Valor Médio por Forma")
        if df_pagto is not None and len(df_pagto) > 0:
            fig = px.bar(df_pagto.sort_values('valor_medio'), y='forma_pagamento', x='valor_medio', orientation='h', title="Valor Médio por Forma")
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Receita Total por Forma")
        if df_pagto is not None and len(df_pagto) > 0:
            fig = px.bar(df_pagto.sort_values('valor_total'), y='forma_pagamento', x='valor_total', orientation='h', title="Receita por Forma")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("#### Quantidade de Transações")
        if df_pagto is not None and len(df_pagto) > 0:
            fig = px.bar(df_pagto, x='forma_pagamento', y='qtd_transacoes', title="Transações por Forma")
            st.plotly_chart(fig, use_container_width=True)

# TAB 6: DADOS BRUTOS
with tab6:
    st.subheader("📊 Dados Brutos")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total de Registros", len(gold_df))
    with col2:
        st.metric("📋 Total de Colunas", len(gold_df.columns))
    with col3:
        st.metric("💾 Tamanho (MB)", f"{gold_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f}")

    st.markdown("#### Colunas Disponíveis")
    st.write(list(gold_df.columns))

    st.markdown("#### Tabela Completa")
    st.dataframe(gold_df, use_container_width=True, height=500)

    st.markdown("#### Estatísticas Descritivas")
    st.dataframe(gold_df.describe(), use_container_width=True)

    csv = gold_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar como CSV",
        data=csv,
        file_name=f"varejo_inteligente_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# TAB 7: MONITORAMENTO DO PIPELINE
with tab7:
    st.subheader("🔧 Monitoramento do Pipeline")

    # Obter estatísticas do pipeline
    pipeline_stats = get_pipeline_stats()

    if pipeline_stats:
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("📂 Registros Bronze", f"{pipeline_stats['bronze']:,}")
        with col2:
            st.metric("✅ Registros Silver", f"{pipeline_stats['silver']:,}")
        with col3:
            st.metric("⭐ Registros Gold", f"{pipeline_stats['gold']:,}")
        with col4:
            taxa_limpeza = ((pipeline_stats['bronze'] - pipeline_stats['silver']) / pipeline_stats['bronze'] * 100) if pipeline_stats['bronze'] > 0 else 0
            st.metric("🧹 Taxa de Limpeza", f"{taxa_limpeza:.1f}%")

        st.markdown("---")

        # Fluxo de dados através das camadas
        st.markdown("#### 📊 Fluxo de Dados por Camada")

        col1, col2 = st.columns(2)

        with col1:
            # Gráfico de barras horizontal
            camadas = ['Bronze', 'Silver', 'Gold']
            registros = [pipeline_stats['bronze'], pipeline_stats['silver'], pipeline_stats['gold']]

            fig = px.bar(
                x=registros,
                y=camadas,
                orientation='h',
                title="Registros por Camada",
                labels={'x': 'Quantidade de Registros', 'y': 'Camada'},
                color=registros,
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Funil de processamento
            fig = px.funnel(
                x=registros,
                y=camadas,
                title="Funil de Processamento"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Informações de timestamp
        st.markdown("#### 📅 Última Atualização por Camada")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**🥉 Bronze**")
            st.caption(f"Timestamp: {pipeline_stats['bronze_timestamp']}")
            st.caption(f"Registros: {pipeline_stats['bronze']:,}")

        with col2:
            st.markdown("**🥈 Silver**")
            st.caption(f"Timestamp: {pipeline_stats['silver_timestamp']}")
            st.caption(f"Registros: {pipeline_stats['silver']:,}")
            if pipeline_stats['bronze'] > 0:
                retencao = (pipeline_stats['silver'] / pipeline_stats['bronze'] * 100)
                st.caption(f"Retenção: {retencao:.1f}%")

        with col3:
            st.markdown("**🥇 Gold**")
            st.caption(f"Timestamp: {pipeline_stats['gold_timestamp']}")
            st.caption(f"Registros: {pipeline_stats['gold']:,}")
            if pipeline_stats['silver'] > 0:
                retencao = (pipeline_stats['gold'] / pipeline_stats['silver'] * 100)
                st.caption(f"Retenção: {retencao:.1f}%")

        st.markdown("---")

        # Resumo do fluxo de dados
        st.markdown("#### 🔄 Resumo do Fluxo de Dados")

        resumo_data = {
            'Etapa': ['Bronze → Silver', 'Silver → Gold', 'Bronze → Gold'],
            'Registros Perdidos': [
                pipeline_stats['bronze'] - pipeline_stats['silver'],
                pipeline_stats['silver'] - pipeline_stats['gold'],
                pipeline_stats['bronze'] - pipeline_stats['gold']
            ],
            'Taxa de Retenção (%)': [
                (pipeline_stats['silver'] / pipeline_stats['bronze'] * 100) if pipeline_stats['bronze'] > 0 else 0,
                (pipeline_stats['gold'] / pipeline_stats['silver'] * 100) if pipeline_stats['silver'] > 0 else 0,
                (pipeline_stats['gold'] / pipeline_stats['bronze'] * 100) if pipeline_stats['bronze'] > 0 else 0
            ]
        }

        df_resumo = pd.DataFrame(resumo_data)
        st.dataframe(df_resumo, use_container_width=True)

        # Informação adicional
        st.info(
            "ℹ️ **Informação**: "
            "A taxa de limpeza representa a porcentagem de registros removidos durante a validação (Bronze → Silver). "
            "A taxa de retenção mostra quantos registros foram mantidos em cada etapa do pipeline."
        )

    else:
        st.warning("⚠️ Não foi possível carregar as estatísticas do pipeline. Verifique se há dados nas camadas Bronze, Silver e Gold.")

# ============================================
# FOOTER
# ============================================
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col2:
    if st.button("🔄 Atualizar Dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.caption(f"Última atualização: {datetime.now().strftime('%H:%M:%S')} | {gold_info}")
