FROM apache/airflow:2.10.4

# Instalar dependências adicionais
# Nota: pandas, requests, pyarrow já vêm no airflow base image
# Apenas adicionar: minio, label-studio-sdk, streamlit e plotly
RUN pip install minio label-studio-sdk streamlit plotly
