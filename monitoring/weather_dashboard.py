import streamlit as st
import pandas as pd
import duckdb as db
import altair as alt

st.set_page_config(page_title="Dashboard de Monitoramento do Tempo")
st.title("Dashboard de Monitoramento do Tempo")

# Conectar duckdb
con = db.connect(r"C:\Users\Maria\Desktop\portifolio\Trabalho_Final_DataOps\data\final_gold.duckdb")
dataset_ouro = con.execute("SELECT * FROM df_ouro").df()
con.close()

# coluna hora para datetime
dataset_ouro["hora"] = pd.to_datetime(dataset_ouro["hora"], errors="coerce")

# coluna de dia/mês
dataset_ouro["dia_mes"] = dataset_ouro["hora"].dt.strftime("%d/%m")

#gráficos com a temperatura máxima e mínima do dia
df_max_dia = dataset_ouro.groupby("dia_mes")["temperatura_c"].max().reset_index()
df_min_dia = dataset_ouro.groupby("dia_mes")["temperatura_c"].min().reset_index()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperatura máxima por dia")
    st.line_chart(df_max_dia, x="dia_mes", y="temperatura_c")

with col2:
    st.subheader("Temperatura mínima por dia")
    st.line_chart(df_min_dia, x="dia_mes", y="temperatura_c")


#gráfico com o índice de uv
st.subheader("Média diária do Índice UV")

# Não quero os valores zero pois eles são de madrugada quando não tem sol
df_uv = dataset_ouro[dataset_ouro["índice_uv"] > 0]

# média por dia
df_uv = (
    df_uv.groupby("dia_mes")["índice_uv"]
    .mean()
    .reset_index()
)

df_uv["cor"] = df_uv["índice_uv"].apply(
    lambda x: "red" if x > 6 else "#66B2FF"   # azul claro
)
chart_uv = (
    alt.Chart(df_uv)
    .mark_bar()
    .encode(
        x=alt.X("dia_mes", title="Dia"),
        y=alt.Y("índice_uv", title="Média do Índice UV"),
        color=alt.Color("cor:N", scale=None)  # mantém as cores exatas definidas
    )
    .properties(height=400)
)

st.altair_chart(chart_uv, use_container_width=True)

#criar duas tebelas se vai nevar ou chover no dia baseados nos dados binários da tabela

st.subheader("Previsão de Chuva por Dia")

df_chuva = dataset_ouro.groupby("dia_mes")["vai_chover"].agg(
    zeros=lambda x: (x == 0).sum(),
    uns=lambda x: (x == 1).sum()
)

# Sim ou Não depende da quantidade de 1 ou 0
df_chuva["Vai chover?"] = df_chuva.apply(
    lambda row: "Sim" if row["uns"] > row["zeros"] else "Não",
    axis=1
)

tabela_chuva = df_chuva[["Vai chover?"]].T

st.table(tabela_chuva)

st.subheader("Previsão de Neve por Dia")

df_neve = dataset_ouro.groupby("dia_mes")["vai_nevar"].agg(
    zeros=lambda x: (x == 0).sum(),
    uns=lambda x: (x == 1).sum()
)

df_neve["Vai nevar?"] = df_neve.apply(
    lambda row: "Sim" if row["uns"] > row["zeros"] else "Não",
    axis=1
)

tabela_neve = df_neve[["Vai nevar?"]].T

st.table(tabela_neve)

#gráfico para comparar o calor com a quantidade de vento no dia

st.subheader("Médias diárias de Vento (kph) e Índice de Calor (°C)")

df_area = dataset_ouro.copy()

# média por dia
df_media_area = (
    df_area.groupby("dia_mes")[["vento_kph", "índice_calor_c"]]
    .mean()
    .reset_index()
)

st.area_chart(
    data=df_media_area,
    x="dia_mes",
    y=["vento_kph", "índice_calor_c"]
)
