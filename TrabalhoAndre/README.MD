# 🎬 Projeto Netflix Data Pipeline (Airflow + Streamlit + MinIO + Postgres)

Este projeto implementa um **pipeline de dados completo** para processar e analisar informações do catálogo da Netflix, usando **Airflow** para orquestração, **Postgres** para armazenamento, **MinIO** como Data Lake e **Streamlit** para visualização.

---

## 🧱 Arquitetura Implementada

### 🔹 Componentes Principais

| Componente | Função |
|-------------|--------|
| **Airflow** | Orquestra o ETL completo dos dados Netflix (ingestão → tratamento → validação → load → relatório). |
| **Postgres** | Banco relacional que armazena os dados tratados. |
| **MinIO (S3 compatível)** | Armazena os arquivos brutos, tratados e relatórios. |
| **Streamlit** | Dashboard interativo para visualização dos dados processados. |
| **Docker Compose** | Gerencia todos os serviços e dependências. |
| **Makefile** | Simplifica os comandos de execução do ambiente. |

---

## ⚙️ Decisões Técnicas

- Uso da **TaskFlow API** no Airflow para uma DAG mais legível e modular.
- Armazenamento de arquivos em **Parquet** para eficiência de leitura e compressão.
- Upload automático dos resultados tratados e relatórios para o **MinIO**.
- Criação automática da tabela `netflix_titles` no **Postgres**.
- Dashboard no **Streamlit** conectado diretamente ao MinIO, carregando os dados via `pandas.read_parquet()`.
- Logs e relatórios de execução são gerados automaticamente e salvos em `/data/processed/relatorios`.

---

## ⚠️ Limitações

- O Metabase está comentado no `docker-compose.yml` (pode ser ativado futuramente).
- A DAG deve ser executada manualmente no Airflow (não possui agendamento automático).
- O ambiente foi projetado para uso **local** via Docker Compose, não para produção.

---

## 🚀 Como Executar Localmente

### 1️⃣ Pré-requisitos

- **Docker** e **Docker Compose** instalados  
- **Make** instalado (já vem em Linux/Mac; no Windows use Git Bash)

### 2️⃣ Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd <PASTA_DO_PROJETO>
```

### 3️⃣ Configurar variáveis de ambiente

Copie o arquivo de exemplo e ajuste se necessário:

```bash
cp infra/.env.example infra/.env
```
💡 As credenciais padrão já funcionam para execução local.

### 4️⃣ Subir o ambiente completo

```bash
make up
```

Isso iniciará os containers:

 - Airflow (webserver, scheduler e logs)

 - Postgres

 - MinIO

 - Streamlit

### 5️⃣ Acessar as interfaces

| Serviço                 | URL                                            | Usuário/Senha               |
| ----------------------- | ---------------------------------------------- | --------------------------- |
| **Airflow**             | [http://localhost:8080](http://localhost:8080) | `admin` / `admin`           |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | –                           |
| **MinIO Console**       | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Postgres**            | `localhost:5432`                               | `airflow` / `airflow`       |

---

## 🔄 Como Executar o Pipeline (Airflow)

 1. Acesse o Airflow em http://localhost:8080

 2. Encontre a DAG fluxo_netflix

 3. Ative e clique em "Trigger DAG"

 4. O pipeline executará as seguintes etapas:

| Etapa                  | Descrição                                    |
| ---------------------- | -------------------------------------------- |
| `listar_csvs`          | Lê os arquivos `.csv` em `data/raw`          |
| `carregar_e_tratar`    | Normaliza e limpa os dados                   |
| `anotar_dados`         | Calcula idade dos filmes e dias desde adição |
| `salvar_parquet`       | Salva dataset tratado em `/data/processed`   |
| `validar_dados`        | Executa validações de qualidade              |
| `enviar_para_minio`    | Envia parquet ao bucket S3 do MinIO          |
| `carregar_no_postgres` | Insere no banco Postgres                     |
| `gerar_relatorio`      | Gera relatório de execução e envia ao MinIO  |

---

## 🧾 Como Validar os Resultados

| Item                      | Onde Verificar                                                                    |
| ------------------------- | --------------------------------------------------------------------------------- |
| **Logs do Airflow**       | Na UI (task logs) ou `airflow/logs/` local                                        |
| **Arquivos tratados**     | `data/processed/netflix_titles_tratado.parquet`                                   |
| **Relatório de execução** | `data/processed/relatorios/relatorio_execucao.csv`                                |
| **Dashboard**             | [http://localhost:8501](http://localhost:8501) (carrega do MinIO automaticamente) |
| **Banco de Dados**        | Conecte-se ao Postgres e veja a tabela `netflix_titles`                           |

--- 

## 🧪 Como Reproduzir um Cenário de Teste

 1. Coloque um ou mais arquivos .csv na pasta data/raw/

 2. Execute o pipeline novamente no Airflow (Trigger DAG)

 3. Verifique:

  - Novos arquivos .parquet em /data/processed/

  - Upload no MinIO (processed/netflix_titles_tratado.parquet)

  - Dashboard atualizado automaticamente

---

## 🧰 Comandos Úteis (Makefile)

| Comando      | Descrição                                       |
| ------------ | ----------------------------------------------- |
| `make up`    | Sobe todo o ambiente (containers Docker)        |
| `make down`  | Derruba todos os containers                     |
| `make init`  | Inicializa apenas o banco Postgres              |
| `make reset` | Derruba tudo, limpa volumes e recria containers |
| `make logs`  | Mostra logs de todos os serviços                |

---

## 🧹 Parar e Limpar o Ambiente
```bash
make down
``` 
Se quiser apagar todos os dados e recomeçar do zero:
```bash
make reset
```

---

## 📊 Dashboard Netflix

O dashboard do Streamlit exibe:

 - Lançamentos por ano

 - Top 10 países com mais títulos

 - Proporção de filmes vs séries

 - Distribuição da idade dos títulos

 - Tabela detalhada com filtros

Acesse em: http://localhost:8501

---

## 🧩 Estrutura do Projeto

```bash
.
├── airflow/
│   ├── dags/                    # DAGs do Airflow (ETL Netflix)
│   ├── app/                     # Código Streamlit (dashboard)
│   ├── logs/                    # Logs de execução
│
├── data/
│   ├── raw/                     # Dados brutos (.csv)
│   ├── processed/               # Dados tratados (.parquet + relatórios)
│
├── infra/
│   ├── docker-compose.yml       # Orquestra os serviços
│   ├── .env.example             # Variáveis de ambiente
│
├── Makefile                     # Comandos simplificados
└── README.md                    # Documentação do projeto

```

