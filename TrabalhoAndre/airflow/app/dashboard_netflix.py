import streamlit as st
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

# ================================================================
# CONFIGURAÇÕES INICIAIS
# ================================================================
st.set_page_config(page_title="Netflix Dashboard", layout="wide", page_icon="🎬")

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
PARQUET_KEY = "processed/netflix_titles_tratado.parquet"

@st.cache_data
def load_data():
    # Caminho no estilo s3://bucket/arquivo
    file_path = f"s3://{MINIO_BUCKET}/{PARQUET_KEY}"

    # Ler parquet diretamente do MinIO
    df = pd.read_parquet(
        file_path,
        storage_options={
            "key": ACCESS_KEY,
            "secret": SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
        },
    )

    # Normalizações
    df.columns = [c.lower().strip() for c in df.columns]

    if 'data_adicao' in df.columns:
        df['data_adicao'] = pd.to_datetime(df['data_adicao'], errors='coerce')

    if 'ano_lancamento' in df.columns:
        df['ano_lancamento'] = pd.to_numeric(df['ano_lancamento'], errors='coerce')

    for col in ['pais', 'tipo']:
        if col not in df.columns:
            df[col] = 'Desconhecido'

    return df.dropna(subset=['pais', 'tipo'])


df = load_data()

# ================================================================
# SIDEBAR - FILTROS
# ================================================================
st.sidebar.header("🔍 Filtros")

paises = sorted(df['pais'].dropna().unique().tolist())
tipos = sorted(df['tipo'].dropna().unique().tolist())

pais_sel = st.sidebar.selectbox("🌍 País", ["Todos"] + paises)
tipo_sel = st.sidebar.multiselect("🎞 Tipo", tipos, default=tipos)

# Filtro aplicado
df_filt = df.copy()
if pais_sel != "Todos":
    df_filt = df_filt[df_filt['pais'] == pais_sel]
if tipo_sel:
    df_filt = df_filt[df_filt['tipo'].isin(tipo_sel)]

# ================================================================
# MÉTRICAS GERAIS
# ================================================================
st.title("🎬 Dashboard Netflix - Dados Processados")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Títulos", len(df_filt))
col2.metric("Total de Filmes", len(df_filt[df_filt["tipo"] == "Movie"]))
col3.metric("Total de Séries", len(df_filt[df_filt["tipo"] == "TV Show"]))
col4.metric("Países Representados", df_filt["pais"].nunique())

st.markdown("---")

# ================================================================
# GRÁFICO 1 - LANÇAMENTOS POR ANO
# ================================================================
if 'ano_lancamento' in df_filt.columns:
    df_ano = df_filt.groupby("ano_lancamento").size().reset_index(name="quantidade")
    fig_ano = px.bar(df_ano, x="ano_lancamento", y="quantidade",
                    title="📅 Lançamentos por Ano",
                    labels={"ano_lancamento": "Ano de Lançamento", "quantidade": "Quantidade"},
                    template="plotly_white")
    st.plotly_chart(fig_ano, use_container_width=True)

# ================================================================
# GRÁFICO 2 - PRINCIPAIS PAÍSES
# ================================================================
df_pais = df.groupby("pais").size().reset_index(name="quantidade").sort_values(by="quantidade", ascending=False).head(10)
fig_pais = px.bar(df_pais, x="pais", y="quantidade",
                title="🌍 Top 10 Países com Mais Títulos",
                color="quantidade", color_continuous_scale="reds",
                template="plotly_white")
st.plotly_chart(fig_pais, use_container_width=True)

# ================================================================
# GRÁFICO 3 - DISTRIBUIÇÃO POR TIPO
# ================================================================
fig_tipo = px.pie(df_filt, names="tipo", title="🎞 Proporção de Filmes vs Séries")
st.plotly_chart(fig_tipo, use_container_width=True)

# ================================================================
# GRÁFICO 4 - IDADE DOS FILMES
# ================================================================
if 'idade_filme' in df_filt.columns:
    fig_idade = px.histogram(df_filt, x="idade_filme",
                              nbins=20,
                              title="📊 Distribuição da Idade dos Filmes/Séries",
                              labels={"idade_filme": "Idade (anos)"},
                              template="plotly_white")
    st.plotly_chart(fig_idade, use_container_width=True)

# ================================================================
# TABELA DETALHADA
# ================================================================
st.markdown("### 📋 Amostra dos Dados Filtrados")
st.dataframe(df_filt.head(50))
