# 🔐 Credenciais de Acesso - Batch Data Pipeline

Este documento contém todas as credenciais de acesso para as ferramentas do pipeline de dados.

---

## 📋 Índice

- [Orquestração](#orquestração)
- [Visualização e BI](#visualização-e-bi)
- [Armazenamento](#armazenamento)
- [Query Engine](#query-engine)
- [Observabilidade](#observabilidade)
- [Data Lineage](#data-lineage)
- [Banco de Dados](#banco-de-dados)
- [Métricas](#métricas)

---

## 🎯 Orquestração

### Apache Airflow

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **URL** | `http://localhost:8080` |
| **Usuário** | `admin` |
| **Senha** | `admin` |
| **Descrição** | Interface web para gerenciar DAGs e monitorar execuções do pipeline |

**Nota:** O Airflow está configurado com LocalExecutor e executa o DAG `sales_delta_pipeline` a cada hora.

---

## 📊 Visualização e BI

### Metabase

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **URL** | `http://localhost:3000` |
| **Email** | `admin@duckmesh.com` |
| **Senha** | `DuckMesh2025!` |
| **Descrição** | Plataforma de Business Intelligence com dashboards pré-configurados |

**Dashboard Pré-configurado:**
- **Nome:** Sales Analytics Dashboard
- **Collection:** Sales Analytics
- **Cards Incluídos:**
  - Total Revenue Today
  - Total Transactions Today
  - Unique Customers Today
  - Completion Rate Today
  - Daily Revenue Trend
  - Top 10 Products by Revenue
  - Customer Value Distribution
  - Revenue by Category

**Conexão com Banco:**
- **Tipo:** Trino
- **Database:** DuckMesh Analytics
- **Schema:** `delta.analytics`

---

### Grafana

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **URL** | `http://localhost:3001` |
| **Usuário** | `admin` |
| **Senha** | `admin` |
| **Descrição** | Visualização de métricas e observabilidade do pipeline |

**Dashboards Pré-configurados:**
- Sales Pipeline Observability Dashboard
- Métricas de saúde do Airflow
- Métricas de MinIO
- Métricas de qualidade de dados

---

## 💾 Armazenamento

### MinIO (S3-compatible Storage)

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **Console URL** | `http://localhost:9001` |
| **API URL** | `http://localhost:9000` |
| **Access Key** | `minioadmin` |
| **Secret Key** | `minioadmin` |
| **Descrição** | Armazenamento de objetos compatível com S3 |

**Buckets Criados:**
- `bronze` - Dados brutos (JSON)
- `silver` - Dados limpos (Delta Lake)
- `gold` - Agregações de negócio (Delta Lake)

**Configuração S3 para DuckDB:**
```python
S3_ENDPOINT: minio:9000
S3_ACCESS_KEY: minioadmin
S3_SECRET_KEY: minioadmin
```

---

## 🔍 Query Engine

### Trino

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **UI URL** | `http://localhost:8081/ui` |
| **API URL** | `http://localhost:8081` |
| **Autenticação** | Não requerida |
| **Descrição** | Motor de consulta SQL distribuído para Delta Lake |

**Conexão via CLI:**
```bash
docker compose exec trino trino --server http://localhost:8081
```

**Catalog Configurado:**
- **Nome:** `delta`
- **Schema:** `analytics`
- **Tabelas Disponíveis:**
  - `daily_sales_summary`
  - `product_performance`
  - `customer_segments`

**Exemplo de Query:**
```sql
USE delta.analytics;
SELECT * FROM daily_sales_summary ORDER BY partition_date DESC LIMIT 10;
```

---

## 📈 Observabilidade

### Prometheus

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **URL** | `http://localhost:9090` |
| **Autenticação** | Não requerida |
| **Descrição** | Coleta e armazenamento de métricas de séries temporais |

**Métricas Coletadas:**
- Métricas do Airflow (via StatsD)
- Métricas do MinIO
- Métricas customizadas do pipeline
- Métricas de qualidade de dados

**Prometheus Pushgateway:**
- **URL:** `http://localhost:9091`
- **Autenticação:** Não requerida
- **Descrição:** Gateway para métricas push do pipeline

---

### StatsD Exporter

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **Metrics Endpoint** | `http://localhost:9102/metrics` |
| **UDP Port** | `9125` |
| **Autenticação** | Não requerida |
| **Descrição** | Exporta métricas do StatsD para Prometheus |

---

## 🔗 Data Lineage

### Marquez (API)

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **API URL** | `http://localhost:5002` |
| **Admin URL** | `http://localhost:5003` |
| **Autenticação** | Não requerida |
| **Descrição** | API para rastreamento de linhagem de dados |

**Endpoints Principais:**
- `GET /api/v1/namespaces` - Listar namespaces
- `GET /api/v1/jobs` - Listar jobs
- `GET /api/v1/datasets` - Listar datasets
- `GET /api/v1/lineage` - Obter linhagem

**Namespace Padrão:**
- `duckmesh-sales`

---

### Marquez Web UI

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **URL** | `http://localhost:3002` |
| **Autenticação** | Não requerida |
| **Descrição** | Interface web para visualizar linhagem de dados |

**Nota:** A porta foi alterada de 5000 para 5002 para evitar conflito com AirPlay Receiver no macOS.

---

## 🗄️ Banco de Dados

### PostgreSQL

| **Propriedade** | **Valor** |
|-----------------|-----------|
| **Host** | `localhost` |
| **Porta** | `5433` |
| **Usuário** | `airflow` |
| **Senha** | `airflow` |
| **Database Airflow** | `airflow` |
| **Database Marquez** | `marquez` |
| **Descrição** | Banco de dados relacional para Airflow e Marquez |

**Conexão via psql:**
```bash
psql -h localhost -p 5433 -U airflow -d airflow
```

**Databases Disponíveis:**
- `airflow` - Metadados do Airflow
- `marquez` - Metadados de linhagem do Marquez

---

## 📊 Métricas e Monitoramento

### Resumo de Portas

| **Serviço** | **Porta** | **Protocolo** |
|-------------|-----------|---------------|
| Airflow Web | 8080 | HTTP |
| Metabase | 3000 | HTTP |
| Grafana | 3001 | HTTP |
| MinIO Console | 9001 | HTTP |
| MinIO API | 9000 | HTTP |
| Trino | 8081 | HTTP |
| Marquez API | 5002 | HTTP |
| Marquez Admin | 5003 | HTTP |
| Marquez Web | 3002 | HTTP |
| Prometheus | 9090 | HTTP |
| Prometheus Pushgateway | 9091 | HTTP |
| StatsD Exporter | 9102 | HTTP |
| StatsD UDP | 9125 | UDP |
| PostgreSQL | 5433 | TCP |

---

## 🔒 Segurança

### ⚠️ Avisos Importantes

1. **Credenciais Padrão:** Todas as credenciais são padrão e devem ser alteradas em ambientes de produção.

2. **MinIO:** As credenciais padrão (`minioadmin/minioadmin`) devem ser alteradas usando variáveis de ambiente:
   ```yaml
   MINIO_ROOT_USER: seu_usuario
   MINIO_ROOT_PASSWORD: sua_senha_forte
   ```

3. **Airflow:** Para alterar as credenciais do Airflow, modifique as variáveis de ambiente:
   ```yaml
   _AIRFLOW_WWW_USER_USERNAME: seu_usuario
   _AIRFLOW_WWW_USER_PASSWORD: sua_senha_forte
   ```

4. **Grafana:** Para alterar as credenciais do Grafana, modifique as variáveis de ambiente:
   ```yaml
   GF_SECURITY_ADMIN_USER: seu_usuario
   GF_SECURITY_ADMIN_PASSWORD: sua_senha_forte
   ```

5. **PostgreSQL:** Para alterar as credenciais do PostgreSQL, modifique as variáveis de ambiente:
   ```yaml
   POSTGRES_USER: seu_usuario
   POSTGRES_PASSWORD: sua_senha_forte
   ```

---

## 🚀 Quick Start

### Acessar todas as ferramentas:

```bash
# Airflow
open http://localhost:8080
# Login: admin / admin

# Metabase
open http://localhost:3000
# Login: admin@duckmesh.com / DuckMesh2025!

# Grafana
open http://localhost:3001
# Login: admin / admin

# MinIO Console
open http://localhost:9001
# Login: minioadmin / minioadmin

# Trino UI
open http://localhost:8081/ui

# Marquez Web
open http://localhost:3002

# Prometheus
open http://localhost:9090
```

---

## 📝 Notas Adicionais

- **Primeira Execução:** O Metabase pode levar alguns minutos para inicializar completamente na primeira vez.
- **Marquez:** O Marquez pode levar até 2 minutos para completar as migrações do banco de dados na primeira inicialização.
- **Trino:** As tabelas Gold só estarão disponíveis após a primeira execução bem-sucedida do DAG do Airflow.
- **Health Checks:** Todos os serviços possuem health checks configurados. Use `docker compose ps` para verificar o status.

---

## 🔄 Atualização de Credenciais

Para atualizar credenciais, edite o arquivo `docker-compose.yml` e reinicie os serviços:

```bash
docker compose down
docker compose up -d
```

---

**Última Atualização:** 2025-11-06  
**Versão do Pipeline:** 1.0.0

