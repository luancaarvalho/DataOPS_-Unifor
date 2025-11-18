import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# ==============================
# FUNÇÃO PARA CARREGAR DADOS
# ==============================
def load_data():
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )

        df = pd.read_sql("SELECT * FROM gold_annotated", conn)
        conn.close()

        # Normalizar annotation
        df["annotation"] = df["annotation"].fillna("neutro").astype(str)

        return df

    except Exception as e:
        st.warning("⚠ Nenhum dado encontrado no banco ainda. Rode a DAG gold_annotated_to_postgres.")
        return None


# ==============================
# INÍCIO DO DASHBOARD
# ==============================
st.set_page_config(page_title="Dashboard Banco Marketing", layout="wide")
st.title("📊 Dashboard – Marketing (Anotações + Gold)")

df = load_data()

if df is not None:
    
    # ==============================
    # GRÁFICO DE PIZZA (5 classes fixas)
    # ==============================

    st.subheader("🎯 Distribuição das Anotações")

    categorias = ["altamente_interessado", "interessado",
                  "neutro", "desinteressado", "rejeitou"]

    contagem = df["annotation"].value_counts().reindex(categorias, fill_value=0)

    fig = px.pie(
        values=contagem.values,
        names=contagem.index,
        title="Distribuição das Anotações (5 classes)",
        color=contagem.index,
        color_discrete_map={
            "altamente_interessado": "#33cc33",
            "interessado": "#66ff66",
            "neutro": "#cccccc",
            "desinteressado": "#ff9933",
            "rejeitou": "#ff3300",
        }
    )
    st.plotly_chart(fig, use_container_width=True)

    # ==============================
    # TABELA DE DADOS BRUTOS
    # ==============================
    st.subheader("📄 Dados Brutos (gold + anotação)")

    colunas_relevantes = [
        "row_id", "idade", "profissao", "estado_civil",
        "escolaridade", "duracao_contato", "num_contatos_campanha",
        "dias_desde_ultimo_contato", "resultado_ultimo_contato",
        "variacao_emprego", "num_empregados", "faixa_idade",
        "faixa_duracao_contato", "intensidade_contato",
        "eficiencia_campanha", "annotation"
    ]

    df_mostrar = df[colunas_relevantes]
    st.dataframe(df_mostrar, use_container_width=True)

else:
    st.info("📭 Aguardando dados serem carregados pelo Airflow.")
