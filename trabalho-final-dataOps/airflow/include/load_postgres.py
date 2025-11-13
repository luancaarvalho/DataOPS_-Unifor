from sqlalchemy import create_engine

def load_to_postgres(df):
    print("Carregando dados no PostgreSQL...")

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )
    df.to_sql("covid_cases", engine, if_exists="replace", index=False)

    print("Dados carregados com sucesso na tabela 'covid_cases'")
