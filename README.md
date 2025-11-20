# Monitoramento de Qualidade do Ar (DataOps + Airflow)

Este projeto implementa um pipeline de **DataOps** para monitorar a qualidade do ar em São Paulo, utilizando a API pública de **Air Quality da Open-Meteo** e orquestração com **Apache Airflow**, em ambiente Docker com Postgres.

O pipeline demonstra:

- Ingestão diária via API externa
- Cálculo de um índice simplificado de qualidade do ar (AQI)
- Geração de flag de risco respiratório
- Aplicação de Data Quality
- Consolidação semanal para BI
- Backfill histórico **idempotente** com `ON CONFLICT`
- Logging técnico em `air_pipeline_log`
- Execução automatizada com Makefile, tox e invoke

---

# 1. Arquitetura em Alto Nível

**Fonte de Dados**
Open-Meteo Air Quality API
Endpoint:
`https://air-quality-api.open-meteo.com/v1/air-quality`

**Orquestração:**
Apache Airflow 2.9.x (Webserver + Scheduler)

**Persistência:**
Postgres 15

**Camadas de Dados:**

- `air_quality_daily` → Fato diário
- `air_dq_results` → Data Quality técnico
- `air_pipeline_log` → Log técnico do pipeline
- `air_bi_view` → Agregação semanal (VIEW)
- `air_quality_bi` → BI semanal consolidado (tabela)

---

# 2. Tecnologias Utilizadas

- Python 3.x
- Apache Airflow
- Airflow Postgres Provider
- Pandas
- Requests
- Docker / Docker Compose
- SQLAlchemy
- Makefile / Invoke / Tox

---

# 3. DAGs do Projeto

## 3.1. DAG Principal — `monitoramento_qualidade_ar`

Pipeline diário com catchup.

### Fluxo:

1. **create_tables**Garante existência das tabelas:

   - `air_quality_daily`
   - `air_dq_results`
   - `air_pipeline_log`
   - `air_quality_bi`
2. **fetch_air_quality**

   - Coleta dados horários da Open-Meteo usando:
     - `start_date = logical_date`
     - `end_date = logical_date`
   - Calcula médias diárias de:
     - PM2.5
     - PM10
     - CO
   - Calcula AQI simplificado:
     `AQI = 2 * PM2.5 + PM10`
3. **annotate**Define:

   - `risk_flag = 1` se `AQI > 80`
   - senão `0`, ou `NULL` se sem dados
4. **save** (idempotente)Usa UPSERT diário com:

   ```
   ON CONFLICT (ref_date)
   DO UPDATE
   ```
5. **dq**Gera métricas técnicas:

   - valores negativos
   - valores nulos
6. **build_bi**
   Cria a view semanal:

   ```
   CREATE OR REPLACE VIEW air_bi_view AS
   SELECT
       date_trunc('week', ref_date)::date AS semana,
       AVG(pm25) AS pm25_media,
       AVG(pm10) AS pm10_media,
       SUM(CASE WHEN risk_flag = 1 THEN 1 ELSE 0 END) AS dias_risco,
       COUNT(*) AS dias_validos,
       SUM(CASE WHEN risk_flag = 1 THEN 1 ELSE 0 END)::float
           / NULLIF(COUNT(*), 0) AS pct_risco
   FROM air_quality_daily
   GROUP BY 1
   ORDER BY semana;
   ```

---

## 3.2. DAG Secundária — `air_quality_relatorios_semanais`

### Função:

Gerar relatórios semanais consolidados EM TABELA, usando dynamic task mapping.

### Fluxo:

1. `get_semanas` → lê semanas da view `air_bi_view`
2. `gerar_relatorio(semana)`
   - calcula `risco_ambiental`
   - atualiza `air_quality_bi`

UPSERT semanal:

```
ON CONFLICT (semana) DO UPDATE
```

---

# 4. Modelagem de Dados

## O que são os dados da API de Qualidade do Ar (Open-Meteo Air Quality API)

A API utilizada neste projeto é a **Open-Meteo Air Quality API**, que fornece previsões e medições de qualidade do ar com granularidade **horária**, para inúmeras regiões do mundo — incluindo São Paulo.

Ela consolida dados de diversas fontes de monitoramento atmosférico, como:

- Modelos atmosféricos globais
- Estações de monitoramento governamentais
- Observações de satélite
- Modelos de dispersão química

Essas fontes são integradas e processadas para gerar variáveis ambientais e poluentes que influenciam diretamente na saúde pública (principalmente me SP que a qualidade do ar é sabidamente ruim).

## Principais Poluentes Utilizados no Projeto

### PM2.5 (Particulate Matter < 2.5 μm)

Partículas ultrafinas que entram profundamente nos pulmões e podem chegar à corrente sanguínea.Variável na API:

- `pm2_5` (μg/m³)

### ✔ PM10 (Particulate Matter < 10 μm)

Partículas inaláveis maiores, como poeira, pó de estrada e resíduos industriais.Variável na API:

- `pm10` (μg/m³)

### ✔ CO – Monóxido de Carbono

Gás tóxico gerado por combustão incompleta.Variável na API:

- `carbon_monoxide` (μg/m³)

## Estrutura dos Dados da API

A API retorna séries horárias contendo arrays paralelos:

- `time[]` → timestamps no formato ISO8601
- `pm10[]` → concentração horária
- `pm2_5[]` → concentração horária
- `carbon_monoxide[]` → concentração horária

Exemplo simplificado:

```
{
  "hourly": {
    "time": ["2025-11-20T00:00", "2025-11-20T01:00", ...],
    "pm10": [18.0, 17.5, ...],
    "pm2_5": [12.0, 11.8, ...],
    "carbon_monoxide": [210.0, 205.0, ...]
  }
}
```

## Como o pipeline usa esses dados

Para cada `logical_date` (incluindo backfills), o Airflow:

1. Solicita dados **apenas daquele dia específico**:

```
start_date = logical_date
end_date = logical_date
```

2. Calcula métricas diárias:

- PM2.5 médio
- PM10 médio
- CO médio
- AQI simplificado:

```
AQI = (2 × PM2.5) + PM10
```

3. Define a flag de risco (`risk_flag`):

- `1` se `AQI > 80`
- `0` se não houver risco
- `NULL` se não houver dados

Esses valores alimentam as estruturas:

- `air_quality_daily` (fato diário)
- `air_bi_view` (view semanal)
- `air_quality_bi` (tabela de BI consolidado)

## Por que esses dados importam?

Segundo a OMS:

- PM2.5 é um dos maiores riscos ambientais à saúde
- Aumenta risco de doenças respiratórias
- Agrava asma e DPOC
- Eleva morbidade e mortalidade em idosos
- Afeta também o sistema cardiovascular

Este pipeline demonstra como um sistema DataOps pode:

- monitorar qualidade do ar diariamente
- identificar dias com potencial risco respiratório
- consolidar relatórios semanais
- alimentar pipelines de análise ou alertas

## 4.1. Tabela `air_quality_daily`

| Campo      | Tipo        |
| ---------- | ----------- |
| id         | SERIAL PK   |
| ref_date   | DATE UNIQUE |
| city       | TEXT        |
| aqi        | NUMERIC     |
| pm25       | NUMERIC     |
| pm10       | NUMERIC     |
| co         | NUMERIC     |
| risk_flag  | INT         |
| created_at | TIMESTAMP   |

## 4.2. Tabela `air_dq_results`

Métricas por execução:

- total_registros
- erros_pm25_neg
- erros_pm10_neg
- erros_aqi_neg
- nulos_pm25
- nulos_pm10
- nulos_aqi

## 4.3. Tabela `air_pipeline_log`

Armazena logs técnicos:

- data_execucao
- task
- status
- mensagem

## 4.4. View `air_bi_view`

Semanal agregada por:

- semana
- pm25_media
- pm10_media
- dias_risco
- dias_validos
- pct_risco

## 4.5. Tabela `air_quality_bi`

UPSERT semanal:

- pm25_media
- pm10_media
- pct_risco
- risco_ambiental (flag)

# 5. requirements.txt

```
apache-airflow==2.9.2
apache-airflow-providers-postgres==5.10.1
pandas==2.2.2
requests==2.32.3
psycopg2-binary==2.9.9
python-dateutil==2.9.0.post0
```

# 6. Subindo o Ambiente

## 6.1. subir o ambiente

```
docker compose up -d
```

## 6.2. inicializar Airflow

```
docker compose run --rm airflow-airflow-webserver-1 airflow db init
```

## 6.3. criar usuário admin

```
docker compose run --rm airflow-airflow-webserver-1 \
airflow users create \
  --username airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password airflow
```

## 6.4. conexão postgres_default

Campos:

- Conn ID: `postgres_default`
- Host: `postgres`
- Porta: `5432`
- Schema: nome do banco
- Login/senha conforme compose

---

# 7. Backfill (Histórico)

## 7.1. Rodar backfill

```
airflow dags backfill monitoramento_qualidade_ar \
  -s 2025-09-01 \
  -e 2025-11-02
```

---

# 8. Scripts de Automação (Makefile, Invoke e Tox)

## 8.1. Makefile (exemplo)

```
up:
	docker compose up -d

down:
	docker compose down

logs-web:
	docker compose logs airflow-airflow-webserver-1 --tail=100 -f

backfill-nov:
	docker compose run --rm airflow-airflow-webserver-1 \
		airflow dags backfill monitoramento_qualidade_ar \
			-s 2025-11-01 \
			-e 2025-11-30
```

## 8.2. invoke — tasks.py

```
from invoke import task

@task
def up(c):
    c.run("docker compose up -d", pty=True)

@task
def backfill_nov(c):
    c.run("docker compose run --rm airflow-airflow-webserver-1 airflow dags backfill monitoramento_qualidade_ar -s 2025-11-01 -e 2025-11-30", pty=True)
```

## 8.3. tox.ini

```
[tox]
envlist = py311, lint
skipsdist = True

[testenv:lint]
deps = flake8
commands = flake8 dags
```

---

# 9. Conclusão

O projeto demonstra:

- pipeline completo de DataOps
- coleta diária via API externa
- idempotência com UPSERT
- Data Quality
- BI semanal
- logging técnico
- automação operacional (Makefile / tox / invoke)
- backfill histórico confiável
- Airflow com Data-Aware Scheduling e Dynamic Task Mapping

Pronto para uso acadêmico, profissional e extensão futura.
