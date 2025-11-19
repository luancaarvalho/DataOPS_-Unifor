# Trabalho Final — Pipeline de Dados SELIC com Airflow

## 1. Introdução

Este projeto implementa um pipeline de dados *end-to-end* orquestrado pelo **Apache Airflow** para monitoramento e enriquecimento de dados financeiros (Taxa SELIC). O objetivo é demonstrar práticas de **DataOps**, incluindo ingestão automatizada, transformação de dados, anotação sintética e armazenamento em Data Lakehouse local (via DuckDB).

O fluxo de dados consiste em:
1.  **Ingestão**: Coleta diária da taxa SELIC via API do Banco Central (BCB).
2.  **Anotação**: Enriquecimento dos dados com uma etapa de anotação sintética (simulando classificação de sentimento ou análise de especialistas).
3.  **Armazenamento**: Persistência dos dados tratados em um banco OLAP local (**DuckDB**).
4.  **Visualização**: Análise exploratória e monitoramento via Jupyter Notebook.

---

## 2. Como Executar

### Pré-requisitos
- Docker e Docker Compose instalados.
- Python 3.10+ (apenas se quiser rodar os notebooks localmente fora do container).

### Passo a Passo

1.  **Subir o Ambiente**
    Navegue até a pasta do projeto e inicie os containers do Airflow:
    ```bash
    cd trabalho_final
    make build up
    ```

2.  **Acessar o Airflow**
    - Abra o navegador em [http://localhost:8080](http://localhost:8080).
    - Credenciais padrão (definidas no `.env` ou `docker-compose.yaml`):
      - Usuário: `airflow`
      - Senha: `airflow`

3.  **Executar o Pipeline**
    - O pipeline está configurado com `catchup=True` e `start_date` em 2024, então ele iniciará automaticamente o processamento do histórico mensal.

    **Opcional: Backfill Manual via CLI**
    Se precisar forçar o reprocessamento ou preencher um período específico via terminal:
    ```bash
    # Acessar o container do scheduler
    docker exec -it trabalho_final-airflow-scheduler-1 bash

    # Criar jobs de backfill (Airflow 2.10+)
    airflow backfill create --dag-id selic_dag --from-date 2024-01-01 --to-date 2025-11-01
    ```

4.  **Visualizar os Dados (BI)**
    Os dados são salvos no arquivo `data/finance.duckdb`. Para visualizar os gráficos e estatísticas:
    - Instale as dependências locais:
      ```bash
      pip install jupyter duckdb pandas matplotlib seaborn
      ```
    - Execute o notebook de BI:
      ```bash
      jupyter notebook notebooks/bi.ipynb
      ```
    - O notebook contém gráficos da evolução da SELIC e a distribuição das anotações geradas.

---

## 3. Limitações e Melhorias Futuras (TODO)

Embora funcional, este projeto possui pontos de melhoria identificados para versões futuras:

- **Validação de Dados (Data Quality)**:
  - A integração completa com **Great Expectations** foi simplificada para validações *inline* (Pandas) para reduzir o tempo de importação da DAG e complexidade de configuração do DataContext.
  - **TODO**: Integrar o `GreatExpectationsOperator` com Checkpoints persistidos.

- **Anotação Real**:
  - Atualmente, a etapa de anotação (`annotate_selic_data`) utiliza uma lógica sintética (random) para fins de demonstração.
  - **TODO**: Integrar com uma API de LLM (OpenAI/Gemini) ou ferramenta de anotação humana (Label Studio).

- **Resiliência**:
  - A API do BCB pode falhar com 429 Too Many Requests caso várias runs estejam acontecendo ao mesmo tempo. Caso o dia não possua valores, pode falhar com 404 Not Found.
  - **TODO**: Implementar lógica de error handling para erros 4xx (400, 404, 429, etc).

- **Idempotência**:
  - A escrita no DuckDB utiliza `INSERT`, o que pode gerar duplicatas se o backfill for rodado múltiplas vezes sem limpeza prévia.
  - **TODO**: Implementar lógica de `MERGE` ou `DELETE` antes do insert para garantir idempotência completa.

