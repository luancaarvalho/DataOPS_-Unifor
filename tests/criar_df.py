#bibliotecas
import sys
import pandas as pd

#conectar com a função do arquivo weather_api
sys.path.append("/opt/airflow")
from data.weather_api import gerar_hoje, gerar_historico

#função para transformar as listas em dada frame
def gerar_df_hist():
    historico = gerar_historico()
    historico_df = pd.json_normalize(historico)

    return historico_df

def gerar_df_hoje():
    hoje = gerar_hoje()
    hoje_df = pd.json_normalize(hoje)
    return hoje_df