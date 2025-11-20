# 📊 Pipeline de Monitoramento de Câmbio, Anotação e BI Semanal
_Projeto DataOps com Airflow, Postgres, Datasets e Dynamic Task Mapping_

---

## ✨ Objetivo Geral

Este projeto implementa um pipeline **end-to-end** para monitorar o câmbio **USD/BRL**, com foco em:

- Ingestão automática de dados via API pública  
- **Anotação automática** dos dados (flag de alerta)  
- **Validação e Data Quality** com logging em tabela dedicada  
- **Geração de visão de negócio semanal** em uma *view* SQL  
- **Relatórios semanais enriquecidos** em tabela física  
- **Monitoramento técnico** do pipeline (status, erros, registros inseridos)  
- Uso de **Airflow Datasets (Data-Aware Scheduling)** para orquestração orientada a dados  
- Uso de **Dynamic Task Mapping** para gerar relatórios semanais de forma escalável

---

## 🧩 Visão das DAGs

O pipeline é composto por **duas DAGs**:

### 1. `monitoramento_cambio_anotacoes` (DAG principal)

Executa diariamente (`@daily`, com `catchup=True`) e é responsável por:

1. **Criação de tabelas** no Postgres (se não existirem):
   - `fx_usdbrl_monitoramento` – fato diária do câmbio  
   - `fx_dq_results` – métricas de Data Quality  
   - `bi_fx_monitoramento_pipeline` – log técnico das execuções  
   - `fx_relatorios_semanais` – tabela final de relatórios semanais (consumida pela DAG 2)

2. **Ingestão de dados da API**  
   - Task: `get_exchange_rates`  
   - Fonte: `https://economia.awesomeapi.com.br/json/daily/USD-BRL/`  
   - Busca os dados do dia baseado em `logical_date` da execução da DAG.

3. **Anotação automática**  
   - Task: `annotate_rates`  
   - Cria a flag:
     ```python
     alert_flag = 1 se bid > LIMITE_ALERTA
     ```
   - O limite é configurável:
     ```python
     LIMITE_ALERTA = 5.35
     ```

4. **Validação + UPSERT no Postgres**  
   - Task: `validate_and_save`  
   - Regras:
     - Se `bid` ou `ask` forem `None` → registra linha “nula” para o dia  
     - Se `bid <= 0` ou `ask <= 0` → gera erro e loga falha  
     - Usa `ON CONFLICT (ref_date)` para garantir **idempotência** por dia:
       ```sql
       INSERT INTO fx_usdbrl_monitoramento (...)
       VALUES (...)
       ON CONFLICT (ref_date) DO UPDATE SET ...
       ```

   - **Emite o Dataset** `FX_MONITORAMENTO_DS`:
     ```python
     FX_MONITORAMENTO_DS = Dataset("postgres://fx_usdbrl_monitoramento")
     ```

5. **Data Quality**  
   - Task: `data_quality`  
   - Lê `fx_usdbrl_monitoramento` e calcula:
     - `total_registros`
     - `erros_bid_nao_positivo`
     - `erros_ask_nao_positivo`
     - `nulos_bid`
     - `nulos_ask`
   - Insere tudo em `fx_dq_results`.

6. **BI – visão semanal**  
   - Task: `build_bi`  
   - Cria a *view* `bi_fx_cambio_negocio`, agregada por semana:
     ```sql
     CREATE OR REPLACE VIEW bi_fx_cambio_negocio AS
     SELECT
         date_trunc('week', ref_date)::date AS semana,
         AVG(bid) AS bid_medio_semana,
         AVG(ask) AS ask_medio_semana,
         SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END) AS dias_alerta,
         COUNT(*) AS dias_com_dado,
         CASE
             WHEN COUNT(*) = 0 THEN 0
             ELSE SUM(CASE WHEN alert_flag = 1 THEN 1 ELSE 0 END)::float / COUNT(*)
         END AS pct_dias_alerta
     FROM fx_usdbrl_monitoramento
     GROUP BY date_trunc('week', ref_date)
     ORDER BY semana;
     ```

   - **Emite o Dataset** `FX_BI_SEMANAL_DS`:
     ```python
     FX_BI_SEMANAL_DS = Dataset("postgres://bi_fx_cambio_negocio")
     ```

7. **Monitoramento técnico**  
   - Task: `monitor_pipeline`  
   - Registra em `bi_fx_monitoramento_pipeline`:
     - data_execucao  
     - task  
     - status  
     - mensagem_erro (se houver)  
     - `nova_linha_fato` (quantidade de registros inseridos/atualizados)

---

### 2. `fx_bi_relatorios_semanais` (DAG de relatórios)

Essa DAG **não tem schedule cron**.  
Ela é disparada **automaticamente por Dataset**, quando a DAG 1 atualiza o BI semanal.

```python
with DAG(
    dag_id="fx_bi_relatorios_semanais",
    start_date=datetime(2025, 11, 1),
    schedule=[FX_BI_SEMANAL_DS],  # data-driven
    catchup=False,
    ...
)
```

Ela executa:

1. **Leitura das semanas disponíveis**
   - Task: `get_semanas_unicas`
   - Lê `bi_fx_cambio_negocio` e extrai:
     ```sql
     SELECT DISTINCT semana FROM bi_fx_cambio_negocio;
     ```
   - Retorna uma lista de semanas em formato string.

2. **Dynamic Task Mapping por semana**
   - Task: `gerar_relatorio_semana`
   - É mapeada dinamicamente:
     ```python
     semanas = get_semanas_unicas()
     gerar_relatorio_semana.expand(semana=semanas)
     ```
   - Para cada semana:
     - Lê a linha correspondente da *view* `bi_fx_cambio_negocio`
     - Calcula `risco_semana`:
       ```python
       risco_semana = 1 se pct_dias_alerta > 0.5, senão 0
       ```
     - Insere/atualiza em `fx_relatorios_semanais`:
       ```sql
       INSERT INTO fx_relatorios_semanais (...)
       VALUES (...)
       ON CONFLICT (semana) DO UPDATE SET ...
       ```

---

## 🗄️ Estrutura das Tabelas Principais

### `fx_usdbrl_monitoramento`
- `id` (PK)  
- `ref_date` (DATE, UNIQUE)  
- `ref_timestamp` (TIMESTAMP)  
- `code`, `codein`, `name`  
- `bid`, `ask`, `pct_change`  
- `alert_flag` (INT)  
- `created_at`  

### `fx_dq_results`
- `data_execucao`  
- `total_registros`  
- `erros_bid_nao_positivo`  
- `erros_ask_nao_positivo`  
- `nulos_bid`  
- `nulos_ask`  

### `bi_fx_monitoramento_pipeline`
- `data_execucao`  
- `execution_date`  
- `task`  
- `status`  
- `mensagem_erro`  
- `nova_linha_fato`  

### `bi_fx_cambio_negocio` (VIEW)
- `semana`  
- `bid_medio_semana`  
- `ask_medio_semana`  
- `dias_alerta`  
- `dias_com_dado`  
- `pct_dias_alerta`  

### `fx_relatorios_semanais`
- `semana` (PK)  
- `bid_medio_semana`  
- `ask_medio_semana`  
- `dias_alerta`  
- `dias_com_dado`  
- `pct_dias_alerta`  
- `risco_semana` (INT: 1 se mais de 50% dos dias da semana foram alerta)  
- `created_at`  

---

## 🔗 Como Funcionam os Datasets e Eventos

Este projeto utiliza **Airflow Datasets** para orquestrar as DAGs de forma **data-driven**.

### Datasets definidos

```python
FX_MONITORAMENTO_DS = Dataset("postgres://fx_usdbrl_monitoramento")
FX_BI_SEMANAL_DS   = Dataset("postgres://bi_fx_cambio_negocio")
```

- `FX_MONITORAMENTO_DS` é emitido pela task `validate_and_save`.  
- `FX_BI_SEMANAL_DS` é emitido pela task `build_bi`.

### Emissão de Dataset (outlets)

Exemplo em `validate_and_save`:

```python
@task(outlets=[FX_MONITORAMENTO_DS])
def validate_and_save(record: dict) -> int:
    ...
```

Exemplo em `build_bi`:

```python
@task(outlets=[FX_BI_SEMANAL_DS])
def build_bi():
    ...
```

### DAG data-driven (consumidora de Dataset)

A DAG `fx_bi_relatorios_semanais` é configurada com:

```python
with DAG(
    dag_id="fx_bi_relatorios_semanais",
    schedule=[FX_BI_SEMANAL_DS],
    ...
)
```

Isso significa que:

- Ela **não é acionada por cron**  
- Ela **é acionada automaticamente** sempre que a DAG 1 atualiza o Dataset `FX_BI_SEMANAL_DS` na execução de `build_bi`.

---

## ⚙️ Como Acionar os Eventos/Datasets na Prática

### 1. Ativar as duas DAGs

No Airflow UI:

- Deixe **`monitoramento_cambio_anotacoes`** como `unpaused`  
- Deixe **`fx_bi_relatorios_semanais`** como `unpaused`  

> Se a DAG de relatórios estiver `paused`, ela **não** será disparada pelo Dataset, mesmo que o Dataset seja atualizado.

---

### 2. Rodar a DAG principal e observar o disparo automático

1. No Airflow UI, clique em:
   - `monitoramento_cambio_anotacoes` → *Run DAG*

2. A execução seguirá a sequência:
   - `create_tables`  
   - `get_exchange_rates`  
   - `annotate_rates`  
   - `validate_and_save` (emite `FX_MONITORAMENTO_DS`)  
   - `data_quality`  
   - `build_bi` (emite `FX_BI_SEMANAL_DS`)  
   - `monitor_pipeline`

3. Quando `build_bi` terminar com sucesso, o Airflow irá:
   - Marcar o Dataset `FX_BI_SEMANAL_DS` como atualizado
   - Disparar automaticamente a DAG `fx_bi_relatorios_semanais`

4. Na DAG 2, você verá:
   - `get_semanas_unicas`  
   - `gerar_relatorio_semana[semana_1]`  
   - `gerar_relatorio_semana[semana_2]`  
   - etc.

### 3. Teste manual de evento/dataset

Você também pode testar apenas a parte de BI + Dataset:

- Execute manualmente **somente** a task `build_bi` da DAG 1:
  - Graph → clique em `build_bi` → *Run*

Isso:

- Recria a view `bi_fx_cambio_negocio`  
- Emite o Dataset `FX_BI_SEMANAL_DS`  
- Dispara automaticamente a DAG `fx_bi_relatorios_semanais`

---

## ✅ Como Validar a Ingestão Automática Ponta a Ponta

Após rodar o pipeline, você pode validar pelo banco (Metabase, Adminer, psql).

### 1. Validar ingestão e anotação diária

```sql
SELECT *
FROM fx_usdbrl_monitoramento
ORDER BY ref_date;
```

Verifique:

- Se há registros para os dias esperados  
- Se `bid`, `ask` e `alert_flag` estão preenchidos  
- Se o dia corrente está presente após a execução da DAG

---

### 2. Validar Data Quality

```sql
SELECT *
FROM fx_dq_results
ORDER BY data_execucao DESC;
```

Verifique:

- `total_registros` crescendo ao longo do tempo  
- `erros_*` normalmente iguais a zero  
- `nulos_*` podem existir se API não retornar dados em algum dia, principalmente dias não-úteis

---

### 3. Validar a visão de negócio semanal (view)

```sql
SELECT *
FROM bi_fx_cambio_negocio
ORDER BY semana;
```

Verifique:

- `bid_medio_semana` e `ask_medio_semana` fazendo sentido  
- `dias_alerta` coerente com a regra de `alert_flag`  
- `pct_dias_alerta` entre 0 e 1

---

### 4. Validar os relatórios semanais enriquecidos (DAG 2)

```sql
SELECT *
FROM fx_relatorios_semanais
ORDER BY semana;
```

Verifique:

- As mesmas métricas da view, agora **materializadas** em tabela  
- Campo `risco_semana`:
  - 1 se `pct_dias_alerta > 0.5` (mais da metade dos dias da semana em alerta)  
  - 0 caso contrário  

Se essas linhas existem e foram preenchidas **sem você rodar manualmente a DAG 2**, então:

- O Dataset `FX_BI_SEMANAL_DS` foi emitido corretamente  
- A DAG `fx_bi_relatorios_semanais` foi disparada automaticamente  
- A ingestão está funcionando ponta a ponta, de forma **data-driven**

---

## 📊 Sugestões de Dashboards (Metabase)

### Visão de Negócio

Base: `bi_fx_cambio_negocio` ou `fx_relatorios_semanais`

- Linha: `bid_medio_semana` vs. `semana`  
- Linha: `ask_medio_semana` vs. `semana`  
- Barra: `dias_alerta` por semana  
- Indicador: `% semanas com risco_semana = 1`

### Monitoramento Técnico

Base: `bi_fx_monitoramento_pipeline` + `fx_dq_results`

- Cards:
  - Total execuções  
  - Média de linhas inseridas  
  - Execuções com erro  
- Séries temporais:
  - `nova_linha_fato` ao longo do tempo  
  - contagem de erros de validação  
- Tabela:
  - Últimas execuções com `status`, `task`, `mensagem_erro`

---

## 🏁 Conclusão

Este projeto demonstra:

- Práticas de **DataOps** aplicadas na prática  
- Orquestração orientada a dados (**Airflow Datasets**)  
- Separação clara entre:
  - Pipeline de ingestão/transformação (DAG 1)  
  - Pipeline de BI/relatório (DAG 2)  
- Uso de **Dynamic Task Mapping** para escalar por semana  
- Observabilidade (tabelas de DQ e monitoramento)  
- Pipeline idempotente (UPSERT por dia)  
- BI atualizado de forma **automática**, sem intervenção manual