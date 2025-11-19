# Pipeline Financeiro com Airflow, SQLite e Superset

## 1. Visão Geral

Este projeto implementa um pipeline de dados financeiros utilizando Apache Airflow para orquestração, SQLite para armazenamento, e Apache Superset para visualização. O objetivo é demonstrar uma arquitetura completa de DataOps, cobrindo ingestão, anotação, validação, armazenamento e monitoramento técnico de tasks.

O pipeline coleta cotações financeiras via yfinance, enriquece os dados com metadados de empresa e setor, executa regras de validação e armazena o resultado final em SQLite. Métricas técnicas de execução de cada task também são registradas em um banco separado, permitindo criação de dashboards no Superset.

## 2. Arquitetura Implementada

### 2.1 Airflow

A DAG pipeline_financeiro executa as etapas:

- Ingestão: Coleta dados históricos via yfinance
- Anotação: Enriquecimento com empresa e setor
- Validação: Verificação de integridade e regras de qualidade
- Armazenamento: Persistência no SQLite
- Métricas: Registro de desempenho e estado de execução de cada task

Scripts organizados dentro de:

```
airflow/include/
    ingestion/download_data.py
    annotate/annotate_data.py
    validate/validate_data.py
    store/store.py
    store/metrics.py
```

### 2.2 Armazenamento

Dados são salvos em:

```
/opt/airflow/data/raw
/opt/airflow/data/processed
/opt/airflow/data/db/finance.db
```

Métricas técnicas são salvas em:

```
/opt/airflow/data/metrics/metrics.db
```

Ambos são acessados pelo Superset via SQLite.

### 2.3 Métricas Registradas

A tabela task_metrics contém:

| Campo       | Descrição                                   |
|-------------|-----------------------------------------------|
| dag_id      | Identificação da DAG                          |
| task_id     | Nome da task                                 |
| run_id      | Execução                                     |
| status      | success ou error                             |
| start_time  | Início da task                               |
| end_time    | Fim da task                                  |
| duration    | Duração em segundos                          |
| timestamp   | Registro da execução                         |

### 2.4 Visualização (Superset)

O Superset conecta diretamente ao SQLite utilizando:

```
sqlite:////app/data/db/finance.db?check_same_thread=false
sqlite:////app/data/metrics/metrics.db?check_same_thread=false
```

Dashboards criados incluem:

- Duração média por task
- Evolução temporal de desempenho
- Contagem de sucesso e erro
- Tabela histórica de execuções

## 3. Como Executar Localmente

### 3.1 Pré-requisitos

- Docker Desktop
- Docker Compose
- Git

### 3.2 Clonar o Repositório

```
git clone <url-do-repositorio>
cd <pasta>
```

### 3.3 Subir o Airflow
 - Atualize o user_id e group_id dentro do arquivo airflow/.env para mapear as permissões corretas do linux
 
```
cd airflow
docker compose up -d
```

Acessar Airflow:

```
http://localhost:8080
```

Credenciais padrão:

```
user: airflow
pass: airflow
```

### 3.4 Subir o Superset

```
docker compose up -d
```

Acessar Superset:

```
http://localhost:8088
```

Login inicial:

```
admin / admin
```

## 4. Como Validar os Resultados

### 4.1 Verificar Pipeline no Airflow

1. Abrir a DAG pipeline_financeiro
2. Ativar a DAG
3. Rodar manualmente
4. Validar:
   - logs das tasks pelo proprio airflow
   - arquivos gerados em data/raw e data/processed
   - atualizações no finance.db

### 4.2 Validar Métricas Técnicas

Após cada execução, o arquivo:

```
/opt/airflow/data/metrics/metrics.db
```

é atualizado com novas entradas.

No Superset:

- Abir o dashboard Finanças e visualize os dados e as métricas coletadas do processo
