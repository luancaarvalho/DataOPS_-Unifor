#bibliotecas
import pandas as pd
import duckdb as db
from datetime import datetime

hoje_prata = "/opt/airflow/data/hoje_silver.duckdb"
historico_prata = "/opt/airflow/data/historico_silver.duckdb"
dataset_ouro = "/opt/airflow/data/final_gold.duckdb"

#conectar aos arquivos prata
con_hoje = db.connect(hoje_prata)
df_hoje = con_hoje.execute("SELECT * FROM hoje_silver").df()
con_hoje.close()

con_historico = db.connect(historico_prata)
df_historico = con_historico.execute("SELECT * FROM historico_silver").df()
con_historico.close()

#juntar as duas tabelas
df_ouro = pd.concat([df_historico, df_hoje], ignore_index=True)

# Adiciona datetiime para identificar a linha mais recente
df_ouro['ingestion_time'] = datetime.now()

# Conectar tabela gold
con_ouro = db.connect(dataset_ouro)

# Registrar o DataFrame como uma tabela temporária
con_ouro.register("temp_df_ouro", df_ouro)

# Criar a tabela se não existir
con_ouro.execute("""
    CREATE TABLE IF NOT EXISTS df_ouro AS SELECT * FROM temp_df_ouro;
""")

# Inserir os dados
con_ouro.execute("""
    INSERT INTO df_ouro SELECT * FROM temp_df_ouro;
""")

# Remover duplicatas com base na coluna 'hora'
dedup_query = """
    CREATE OR REPLACE TABLE temp_dedup AS
    SELECT * EXCLUDE (row_number)
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY hora ORDER BY ingestion_time DESC) as row_number
        FROM df_ouro
    )
    WHERE row_number = 1;

    DROP TABLE df_ouro;
    ALTER TABLE temp_dedup RENAME TO df_ouro;
"""
con_ouro.execute(dedup_query)

# Fechar conexão
con_ouro.close()

print("Carregamento da tabela ouro concluído")
