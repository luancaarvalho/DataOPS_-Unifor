#bibliotecas
import sys
import pandas as pd
import duckdb

#conectar com a função do arquivo weather_api
sys.path.insert(0, "/opt/airflow")
from tests.criar_df import gerar_df_hist, gerar_df_hoje

def salvar_bronze():
    try:
        # chamar as funções do script criar_df.py
        historico_df = gerar_df_hist()
        hoje_df = gerar_df_hoje()

        if historico_df.empty:
            raise ValueError("gerar_df_hist retornou um DataFrame vazio")
        if hoje_df.empty:
            raise ValueError("gerar_df_hoje retornou um DataFrame vazio")

        # salvar dataframe historico como camada bronze em duckdb
        hist_bronze = "/opt/airflow/data/historico_bronze.duckdb"
        con = duckdb.connect(hist_bronze)
        con.register("df_hist", historico_df)

        '''con.execute("""
            CREATE TABLE IF NOT EXISTS historico_bronze AS
            SELECT * FROM df_hist
            LIMIT 0
        """)
        con.execute("INSERT INTO historico_bronze SELECT * FROM df_hist")
        con.close()'''
        
        con.execute(f"CREATE TABLE IF NOT EXISTS historico_bronze AS SELECT * FROM df_hist LIMIT 0")
        con.execute("INSERT INTO historico_bronze SELECT * FROM df_hist")

        print("Histórico bronze salvo")

        # repetir o processo para salvar os dados de hoje
        hoj_bronze = "/opt/airflow/data/hoje_bronze.duckdb"
        con = duckdb.connect(hoj_bronze)
        con.register("df_hoje", hoje_df)

        ''' con.execute("""
            CREATE TABLE IF NOT EXISTS hoje_bronze AS
            SELECT * FROM df_hoje
            LIMIT 0
        """)
        con.execute("INSERT INTO hoje_bronze SELECT * FROM df_hoje")
        con.close()'''

        con.execute(f"CREATE TABLE IF NOT EXISTS hoje_bronze AS SELECT * FROM df_hoje LIMIT 0")
        con.execute("INSERT INTO hoje_bronze SELECT * FROM df_hoje")

        print("Hoje (bronze) salvo")

    except Exception as e:
        print(f"Erro em salvar_bronze: {e}")
        raise

salvar_bronze()