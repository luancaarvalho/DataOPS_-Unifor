# DataOps Pipeline – Monitoramento do Tempo

Pipeline completo de ingestão, processamento, validação e visualização de dados meteorológicos usando **Python, DuckDB, Airflow, Great Expectations, Streamlit e Docker**.

## Arquitetura do Projeto
Trabalho_Final_DataOps/
│
├── data/ # Arquivos Bronze, Silver e Gold (DuckDB)
├── tests/ # Scripts de extração e transformação
├── monitoring/ # Dashboard Streamlit
├── dags/ # DAGs do Apache Airflow
├── logs/ # Logs gerados automaticamente pelo Airflow
├── plugins/ # Plugins customizados (opcional)
├── Dockerfile
├── docker-compose.yml
└── README.md

##  Descrição Geral

O pipeline possui **7 etapas**, controladas por um DAG:

1. Extração da API (`weather_api.py`)
2. Criação de DataFrames (`criar_df.py`)
3. Camada Bronze (`extrair_bronze.py`)
4. Validação Silver (`validacao_prata_gx.py`)
5. Camada Silver (`extrair_prata.py`)
6. Camada Gold (`extrair_ouro.py`)
7. Dashboard (`weather_dashboard.py`)

## Bronze • Silver • Gold

### Bronze  
- Dados crus vindos da API  
- Guardados em DuckDB sem transformação  

### Silver  
- Dados limpos, normalizados e validados  
- Great Expectations garante qualidade  

### Gold  
- Dados prontos para consumo  
- Utilizados diretamente pelo Streamlit 

### Docker Compose — Recursos principais

Banco Postgres para metadados do Airflow
Airflow Scheduler
Airflow Webserver
Airflow Init
Instalação automática de:
- pandas
- requests
- duckdb
Todos os diretórios principais são montados nos containers:

./dags        → /opt/airflow/dags
./tests       → /opt/airflow/tests
./data        → /opt/airflow/data
./monitoring  → /opt/airflow/monitoring
./logs        → /opt/airflow/logs
./plugins     → /opt/airflow/plugins


### Qualidade dos Dados — Great Expectations

Validações usadas no Silver:
Intervalo de temperaturas
Booleans (0/1): vai_chover, vai_nevar, é_dia
indice_uv >= 0
Colunas essenciais não nulas
Se falhar → DAG falha automaticamente.

### Observabilidade

Além dos logs padrão do Airflow, o projeto pode:
Salvar métricas de DAG/tarefas em DuckDB
Registrar tempo de execução
Registrar falhas
Criar painéis próprios no Streamlit