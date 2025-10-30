# Trabalho Final — Fluxo de Monitoramento de Anotações com Airflow

## 1. Objetivo Geral
Desenvolver um pipeline orquestrado via **Apache Airflow** para **monitorar anotações end-to-end**, integrando:
1) **Ingestão**,
2) **Transformação e Qualidade de Dados**,
3) **Visualização (BI)** com visão de negócio e de monitoramento.

O trabalho deve evidenciar práticas de **DataOps**: automação, versionamento, observabilidade e reprodutibilidade.

---

## 2. Repositório da Disciplina e Entrega (obrigatório)
- O trabalho **deve ser entregue via Pull Request (PR)** no repositório oficial da disciplina:  
  **https://github.com/luancaarvalho/DataOPS_-Unifor**
- Fluxo de entrega:
  1. Faça um **fork** do repositório acima.
  2. Crie uma **branch** no seu fork seguindo o padrão: `trabalho-final/<seu-nome>`  
     Exemplo: `trabalho-final/maria-silva`
  3. Implemente sua solução no seu fork e branch.
  4. Abra um **Pull Request** do seu fork/branch para o repositório oficial.
- Avaliação: **eu vou clonar seu PR** e a **nota** será atribuída com base no que foi entregue no PR (código, documentação, execução).
- É **obrigatório** incluir um **README.md** principal no seu PR descrevendo:
  - O que foi implementado (arquitetura, decisões, limitações).
  - **Comandos para executar** localmente (passo a passo).
  - Como validar os resultados (onde ver dashboards, relatórios e logs).
  - Como reproduzir o cenário de teste/eventos (se aplicável).

---

## 3. Requisitos Técnicos

### 3.1 Orquestração com Airflow
- Todo o fluxo deve ser orquestrado via **Apache Airflow**.
- Estruture em DAG(s) as seguintes etapas mínimas:
  - **Ingestão de anotações** (batch ou event-driven).
  - **Transformação** e **Validação de Qualidade** (Data Quality).
  - **Atualização do BI** (refresh de modelos/tabelas/dashboards).
- O pipeline deve expor logs, status de tarefas, métricas de sucesso/erro e permitir reexecução controlada.
- Utilize a versão estável mais recente do **Apache Airflow 2.x** (>= 2.10). Você pode reutilizar o deployment base disponível em [aula_01/airflow](https://github.com/luancaarvalho/DataOPS_-Unifor/tree/main/aula_01/airflow) e estendê-lo conforme necessário.

### 3.2 Ingestão de Dados
- Você pode usar **qualquer dataset** e **qualquer ferramenta de anotação** (manual ou automática), por exemplo:
  - Label Studio, Snorkel, Prodigy, APIs de LLMs, serviços próprios, etc.
- Entregáveis mínimos:
  - Rotina de extração/leitura do dado anotado.
  - Normalização/validação de esquema.
  - Armazenamento estruturado (Parquet/Delta/Iceberg/PostgreSQL, etc.).

### 3.3 Transformação e Qualidade
- Aplique transformações necessárias para consumo analítico.
- Inclua **Data Quality** (ex.: Great Expectations/Soda Core ou validações próprias) cobrindo:
  - Regras de integridade (not null, unique, ranges).
  - Relatórios de qualidade e falhas tratadas.

### 3.4 Visualização (BI)
- Entregar duas visões:
  - **Visão de negócio** (ex.: produtividade de anotação, distribuição de classes, taxa de concordância, etc.).
  - **Visão de monitoramento técnico** (ex.: tempos de execução, taxa de falha por tarefa, latência por estágio).
- Pode usar Metabase, Superset, Grafana, Power BI, ou similar. O pipeline deve **atualizar** as fontes do BI.

### 3.5 Bônus (opcional, pontuação extra)
- Implementar **orquestração data-driven** com **Airflow Datasets** (Data-Aware Scheduling), conectando ingestão → transformação → BI a partir de artefatos de dados.
- Explorar sensores e operadores **deferrable** para reduzir consumo de recursos durante espera por eventos.
- Utilizar **Dynamic Task Mapping** ou **TaskFlow API** para escalabilidade e parametrização avançada.
- Integrar **ingestão event-driven** (ex.: novos arquivos em S3/MinIO, webhook, fila Kafka/Pub/Sub, TriggerDagRunOperator).
- Evidenciar no README como acionar os eventos/datasets e validar a ingestão automática ponta a ponta.

---

## 4. Ferramentas (sugestões)
- Orquestração: **Apache Airflow** (obrigatório).
- Anotação: Label Studio, Snorkel, Prodigy, LLM APIs.
- Ingestão: Airbyte/PyAirbyte, conectores próprios, requests/SDKs.
- Transformação: Pandas, dbt, PySpark, DuckDB.
- Data Quality: Great Expectations, Soda Core, testes Python.
- Storage: MinIO/S3, Delta Lake, Iceberg, PostgreSQL.
- BI: Metabase, Superset, Grafana, Power BI.
- Observabilidade: OpenLineage/Marquez, logs do Airflow; opcional: OTEL/Prometheus.
- Infra local: Docker Compose (recomendado).

---

## 5. Critérios de Avaliação

| Critério | Descrição | Peso |
|---|---|---|
| Orquestração e automação | DAG(s) bem definidas, dependências corretas, reexecução e parametrização | 25% |
| Transformação e qualidade | Boas práticas de modelagem e validações de dados reprodutíveis | 25% |
| Visualização e análise | Dashboards funcionais (negócio e monitoramento) atualizados pelo pipeline | 20% |
| Entrega e versionamento | PR no repositório oficial, organização do código, README claro | 15% |
| Bônus (event-driven) | Ingestão dirigida por eventos, documentação e demonstrativo | 15% |

Observações:
- O **README** é parte essencial da nota. Sem README claro e comandos executáveis, há desconto significativo.
- A reprodutibilidade e a clareza do setup local serão consideradas.

---

## 6. Dicas de Execução Local
- Forneça um **docker-compose.yml** para subir seus serviços (Airflow, banco, MinIO, BI).
- Inclua **scripts de bootstrap** (ex.: `make up`, `make down`, `make reset`) ou instruções equivalentes.
- Documente variáveis de ambiente necessárias em `.env.example`.
- Inclua dados de exemplo minimamente suficientes para validar o fluxo.

---

## 7. Exemplo de Estrutura de Repositório
```text
.
├── airflow/                 # Deploy base (docker-compose, configs, plugins)
│   ├── dags/                # DAGs principais com TaskFlow API/Dynamic Mapping
│   ├── datasets/            # Definições de Airflow Datasets (data-driven)
│   ├── include/             # Scripts auxiliares, SQL, modelos de qualidade
│   └── plugins/             # Operadores, hooks e sensores customizados
├── data/                    # Dados brutos, camadas tratadas e artefatos de BI
├── docs/                    # Documentação adicional, diagramas, screenshots
├── monitoring/              # Dashboards, configs de observabilidade/OpenLineage
├── notebooks/               # Explorações e protótipos (opcional)
└── tests/                   # Testes unitários/de integração (p.ex. pytest, GE)
```

- Mantenha artefatos versionados e parametrizados via `.env` ou `config/`.
- Documente no README principal como os datasets e ativos são produzidos e consumidos.
- Inclua scripts de automação (`Makefile`, `tox`, `invoke`) para padronizar execução.

---

## 8. Referências e Boas Práticas Atualizadas
- [Airflow Datasets & Data-Aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/datasets.html)
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html) e TaskFlow API.
- [Deferrable Operators & Triggers](https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html) para sensores/eventos eficientes.
- [OpenLineage Integration](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html) para rastrear lineage e metadados.
- [Airflow 2.10 Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html) — acompanhe features recém-lançadas.
