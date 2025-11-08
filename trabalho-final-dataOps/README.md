# 🧬 Pipeline de Dados COVID-19 — Ceará

### Projeto Final — DataOps / Engenharia de Dados

## Objetivo

Este projeto implementa um **pipeline orquestrado com Apache Airflow** para **monitorar dados de COVID-19 do estado do Ceará**, utilizando dados públicos da [API Brasil.io](https://brasil.io/covid19/cities/cases/).

O pipeline realiza o fluxo **end-to-end** de **ingestão, transformação, validação e visualização em tempo real** no **Grafana**, integrando as seguintes etapas:

- Ingestão de dados via API pública
- Transformação e cálculo da média móvel
- Validação de qualidade (nulos e negativos)
- Armazenamento no PostgreSQL
- Visualização automática no Grafana
- Orquestração e logs via Apache Airflow

## Estrutura do Projeto

trabalho-final-dataOps/
│
├── airflow/
│ ├── dags/
│ │ └── covid_pipeline_dag.py
│ ├── include/
│ │ ├── ingestion.py
│ │ ├── transform.py
│ │ └── load_postgres.py
│ ├── logs/
│ └── docker-compose.yml
│
├── grafana/
│ ├── dashboards/
│ │ └── covid_dashboard.json
│ └── provisioning/
│ ├── dashboards/
│ │ └── covid-dashboard.yml
│ └── datasources/
│ └── postgres.yml
│
├── data/
│ ├── raw/
│ │ └── covid_data.csv
│ └── processed/
│ └── covid_transformed.csv
│
├── Dockerfile
└── README.md

## Pipeline (Airflow DAG)

**DAG:** `covid_pipeline_dag`

| Etapa                | Descrição                                           |
| -------------------- | --------------------------------------------------- |
| `wait_for_data`      | Aguarda a presença do arquivo CSV de dados brutos   |
| `ingest_data`        | Faz a coleta dos dados via API Brasil.io            |
| `validate_data`      | Verifica valores nulos e inconsistências            |
| `transform_and_load` | Agrega, calcula média móvel e carrega no PostgreSQL |
| `notify_grafana`     | Indica a atualização do dashboard no Grafana        |

## Tecnologias Utilizadas

| Camada                | Ferramenta           |
| --------------------- | -------------------- |
| **Orquestração**      | Apache Airflow 2.10  |
| **Ingestão**          | Requests             |
| **Transformação**     | Pandas               |
| **Data Quality**      | Validação automática |
| **Banco de Dados**    | PostgreSQL           |
| **Visualização (BI)** | Grafana              |
| **Infraestrutura**    | Docker Compose       |
