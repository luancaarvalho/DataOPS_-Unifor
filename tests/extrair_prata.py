import duckdb
import sys
import os

#sys.path.append("/opt/airflow")
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'tests'))
from validacao_prata_gx import transformar_prata

bronze_historico = "/opt/airflow/data/historico_bronze.duckdb"
bronze_hoje = "/opt/airflow/data/hoje_bronze.duckdb"

silver_historico = "/opt/airflow/data/historico_silver.duckdb"
silver_hoje = "/opt/airflow/data/hoje_silver.duckdb"



def processar_tabela(bronze_path, silver_path, bronze_table, silver_table):
    
    # ler arquivo bronze
    con_bronze = duckdb.connect(bronze_path)
    df = con_bronze.execute(f"SELECT * FROM {bronze_table}").df()
    con_bronze.close()

    if df.empty:
        print(f"DataFrame Bronze para '{bronze_table}' está vazio. Nenhuma transformação/validação será executada.")
        return

    # transformação e validação com GE
    df_silver, resultados = transformar_prata(df)

    # Não salvar se a validação não funcionar
    if not resultados.success:
        print(f"Falha em {bronze_table}. Nada foi salvo.")
        return

    print(f"Arquivo sem erros")

    # salvar arquivos prata
    con_silver = duckdb.connect(silver_path)
    con_silver.register("temp", df_silver)

    existe = con_silver.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = '{silver_table}'
    """).fetchall()
    #se o arquivo já existir vai adicionar as linhas
    if existe:
        con_silver.execute(f"INSERT INTO {silver_table} SELECT * FROM temp")
        print(f"Append realizado em {silver_table}")
    else:
        con_silver.execute(f"CREATE TABLE {silver_table} AS SELECT * FROM temp")
        print(f"Tabela {silver_table} criada")

    con_silver.close()

#chamar a função
def salvar_prata():
    processar_tabela(bronze_historico, silver_historico, "historico_bronze", "historico_silver")
    processar_tabela(bronze_hoje, silver_hoje, "hoje_bronze", "hoje_silver")

if __name__ == "__main__":
    salvar_prata()
