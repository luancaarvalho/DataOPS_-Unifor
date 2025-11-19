# Fluxo de Monitoramento de Anotações com Apache Airflow

## 1. Visão Geral

Este projeto implementa um **pipeline de dados orquestrado com Apache Airflow** para monitorar, de ponta a ponta, um fluxo de **anotações de reviews de e-commerce**.

O objetivo é:
- Ingerir reviews de produtos e suas respectivas anotações (sentimento e tema).
- Transformar e validar os dados para consumo analítico.
- Atualizar visões de **negócio** (qualidade das reviews, distribuição de sentimentos, produtividade de anotação) e de **monitoramento técnico** (linhas processadas, falhas de qualidade, execução do pipeline).
- Evidenciar práticas de **DataOps**: automação, versionamento, observabilidade e reprodutibilidade.

---

## 2. Arquitetura da Solução

### 2.1 Visão de Alto Nível

Fluxo principal:

1. **Ingestão**
   - Leitura de arquivos CSV com:
     - `reviews.csv`: reviews de produtos.
     - `annotations.csv`: anotações de sentimento/tema para cada review.
   - Armazenamento em tabelas brutas no Postgres: `raw_reviews` e `raw_annotations`.

2. **Transformação + Qualidade**
   - Junção de reviews e anotações em `fact_review_annotations`.
   - Regras de **Data Quality**:
     - `review_id` não nulo.
     - `sentimento` dentro da lista esperada (`positivo`, `neutro`, `negativo`).
     - Consistência básica entre reviews e anotações.
   - Registro de métricas simples de qualidade (ex.: falhas/linhas processadas).

3. **Camada de BI**
   - Visão de negócio:
     - Percentual de sentimento por produto e por dia.
     - Volume de reviews anotadas.
   - Visão de monitoramento:
     - Linhas processadas em `fact_review_annotations`.
     - Histórico de execuções em `bi_monitoramento_pipeline`.
   - O Metabase é usado para montar dashboards em cima dessas tabelas.

### 2.2 Componentes

- **Orquestração:** Apache Airflow 2.10.x
- **Banco de Dados:** PostgreSQL
- **BI:** Metabase
- **Storage de arquivos:** pasta local `data/` montada no container do Airflow.

### 2.3 Estrutura de Pastas

```text
.
├── airflow/
│   ├── docker-compose.yaml     # Compose com Airflow, Postgres e Metabase
│   ├── dags/
│   │   ├── dag_monitoramento_anotacoes.py
│   │   └── datasets_definitions.py      # (opcional: Airflow Datasets)
│   ├── include/
│   │   ├── sql/
│   │   │   ├── create_tables.sql
│   │   │   └── bi_views.sql
│   │   ├── dq_rules.py         # Regras de Data Quality auxiliares
│   │   └── utils.py
│   ├── plugins/
│   └── requirements.txt
├── data/
│   ├── raw/
│   │   ├── reviews.csv
│   │   └── annotations.csv
│   ├── bronze/
│   ├── silver/
│   └── bi/
├── docs/
│   ├── arquitetura.png         
│   └── descricao_dataset.md
├── monitoring/
│   └── metabase/
│       └── dashboards_screenshots.md
├── .env.example
└── README.md
