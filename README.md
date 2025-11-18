# DataOps Pipeline - Bronze, Silver, Gold

Pipeline completo de dados com arquitetura em camadas, orquestração Airflow e dashboard em tempo real.

## Arquitetura

```
Label Studio (NER Annotations)
         ↓
    Bronze Layer (Raw JSON)
         ↓
    Silver Layer (Clean + NER Extraction)
         ↓
    Gold Layer (Aggregations + KPIs)
         ↓
    Streamlit Dashboard
```

## Stack Tecnológica

- **Orquestração**: Apache Airflow (event-driven com sensores deferráveis)
- **Storage**: MinIO (S3-compatible, 3 camadas)
- **Transformação**: Python + Pandas
- **Anotação**: Label Studio (NER)
- **Visualização**: Streamlit
- **Infraestrutura**: Docker + Docker Compose
- **Banco de Dados**: PostgreSQL

## Quick Start (Usando Makefile)

> **Primeira vez usando?** Veja [INSTALACAO_AMBIENTE.md](INSTALACAO_AMBIENTE.md) para instalação completa do zero (Conda + UV + Docker)

### Opção 1: Com Makefile (Recomendado) ⚡

**Setup em 2 comandos:**

```bash
# 1. Clone o repositório
git clone <URL>
cd Dataops

# 2. Suba a infraestrutura (configura .env automaticamente)
make up
```

O comando `make up` irá:
- ✅ Criar arquivo `.env` com credenciais padrão (se não existir)
- ✅ Subir todos os containers Docker
- ✅ Configurar MinIO, Airflow, Label Studio e Dashboard

**Acesse os serviços:**
- **Airflow**: http://localhost:8080 (airflow/airflow)
- **Label Studio**: http://localhost:8001 (admin@localhost.com/123456)
- **MinIO**: http://localhost:9001 (dataops_admin/DataOps2025!SecurePassword)
- **Dashboard**: http://localhost:8501

```bash
# 3. Configure Label Studio token (veja seção abaixo)
# Edite .env e adicione seu LABELSTUDIO_TOKEN

# 4. Execute o pipeline
make run

# 5. Visualize o dashboard
make dashboard
```

**Ambiente Python (opcional - apenas para desenvolvimento local):**
```bash
make setup
conda activate dataops
```

### Opção 2: Comandos Manuais

```bash
# 1. Clone o repositório
git clone <URL>
cd Dataops

# 2. Configure o ambiente Python (opcional - local)
conda create -n dataops python=3.10 -y
conda activate dataops
uv sync --directory environments

# 3. Configure as credenciais (OPCIONAL - já sobe com padrão)
cp .env.example .env
# Edite .env apenas se quiser alterar credenciais

# 4. Inicie o ambiente Docker
docker-compose up -d

# 5. Acesse os serviços (mesmas URLs acima)
```

### Comandos do Makefile Disponíveis

```bash
make          # Ver todos os comandos disponíveis
make setup    # Criar ambiente conda + instalar dependências
make install  # Instalar/atualizar dependências
make up       # Subir containers Docker
make down     # Parar containers
make restart  # Reiniciar containers
make logs     # Ver logs dos containers
make clean    # Limpar buckets MinIO
make run      # Executar pipeline completo
make dashboard # Abrir dashboard Streamlit
make test     # Rodar testes
make lint     # Verificar código
```

## Documentação Completa

### Para Começar
- **[INSTALACAO_AMBIENTE.md](INSTALACAO_AMBIENTE.md)** - Instalação completa do ambiente do zero
  - Instalar Conda/Miniconda
  - Criar ambiente Python 3.10
  - Instalar UV (gerenciador de dependências)
  - Instalar Docker + Docker Compose
  - Configurar e executar todo o pipeline

- **[SETUP_COMPLETO.md](SETUP_COMPLETO.md)** - Guia rápido (se já tem ambiente)
  - Pré-requisitos
  - Instalação
  - Configuração do Legacy Token do Label Studio
  - Execução do pipeline
  - Troubleshooting

## IMPORTANTE: Label Studio Legacy Token e Project ID

Este pipeline requer o **Legacy Token** do Label Studio, não o Access Token (JWT).

**Tutorial em vídeo**:
- Como criar o Legacy Token: https://drive.google.com/file/d/11teN7OjPgbhWD17H0z4XPJ5pYhE3D4_j/view?usp=sharing
- Como criar o projeto: https://drive.google.com/file/d/1sC-S7fQ0PFElqM8oX01OP-f2IsGlSrx_/view?usp=sharing

**Como configurar e obter**:

1. **Acesse Label Studio**: http://localhost:8001
2. **Primeiro acesso**: Crie conta com `label_ops@gmail.com` / `dataops@123`
   - Ou use credenciais padrão: `admin@localhost.com` / `123456`

3. **Habilitar Legacy Tokens (evitar expiração)**:
   - Clique em **"Organization"** (menu lateral)
   - Clique em **"API Tokens Settings"**
   - Deixe APENAS a flag **"Legacy tokens"** marcada
   - Desmarque as outras opções
   - Clique em **"Save"**

4. **Copiar o token**:
   - Clique no ícone do usuário (canto superior direito)
   - **Account & Settings**
   - Procure por **"Legacy API Token"**
   - Copie o token (40 caracteres hexadecimais)

5. **Obter ID do Projeto**:
   - Acesse o projeto no Label Studio
   - Veja a URL do navegador: `http://localhost:8001/projects/3/data?tab=3`
   - O número após `/projects/` é o ID do projeto (neste exemplo: `3`)

6. **Inserir no arquivo .env**:
   ```env
   LABELSTUDIO_TOKEN=seu_token_aqui_40_caracteres
   LABELSTUDIO_PROJECT=3  # ID do seu projeto
   ```

Ver detalhes completos em **[SETUP_COMPLETO.md](SETUP_COMPLETO.md) - Passo 4.3**

## Dataset do Projeto

O projeto utiliza um dataset de transações comerciais com anotações NER.

**Baixar dataset**:
```
https://drive.google.com/drive/folders/1WFkw54HojR1y_Io26_cNV5ni3888I2FZ?usp=sharing
```

**Conteúdo**:
- **1000 registros totais** de transações comerciais
  - 500 registros completos e válidos (cliente, produto, valor, etc.)
  - 500 registros incompletos ou com falhas (para testar validação)
- Anotações NER já realizadas
- Pronto para importação no Label Studio
- **Arquivo de teste event-driven**: Inclui arquivo JSON para testar detecção automática de novos arquivos no bucket Bronze (inbox)

**Propósito do dataset misto**:
- Testar a **camada Silver** com validações de qualidade de dados
- Demonstrar **pipeline robusto** que identifica e remove dados inválidos
- Calcular **métricas de qualidade** (taxa de limpeza, retenção)
- **Testar event-driven**: Arquivo adicional para simular chegada de novos dados no inbox

**Como usar**:
1. Baixe o dataset do Google Drive
2. Importe no Label Studio (Project ID 4)
3. Execute o pipeline via Airflow
4. Visualize resultados no Dashboard

**Resultado esperado após processamento**:
- Bronze: 1000 registros (dados brutos)
- Silver: ~500 registros (após validação)
- Gold: ~500 registros (agregados)
- Taxa de limpeza: ~50%

Ver instruções completas em **[INSTALACAO_AMBIENTE.md](INSTALACAO_AMBIENTE.md) - Parte 8.6**

## Principais Features

- **Event-Driven Architecture** - Airflow detecta novos arquivos automaticamente
- **3 Camadas (Bronze/Silver/Gold)** - Arquitetura Medallion completa
- **Validação de Qualidade de Dados** - Pipeline remove ~50% de registros inválidos
- **NER Extraction** - Named Entity Recognition via Label Studio
- **Monitoramento de Pipeline** - Dashboard com métricas Bronze/Silver/Gold
- **Debug Automático** - Logs detalhados de extração e validação
- **Segurança** - Credenciais em variáveis de ambiente (zero hardcoded)
- **Auto-detecção de Ambiente** - Funciona local e Docker sem mudanças
- **Dashboard Real-time** - Streamlit com atualização automática (TTL 60s)

## Métricas

- **1000 registros** no dataset (500 válidos + 500 inválidos)
- **~50% taxa de limpeza** (validação Silver remove registros problemáticos)
- **8 KPIs** pré-calculados na camada Gold
- **3 camadas** de storage (Bronze/Silver/Gold)
- **0 credenciais** hardcoded (100% variáveis de ambiente)
- **100% containerizado** (Docker + Docker Compose)

## Desenvolvimento

### Automação com Makefile

O projeto inclui um **Makefile** para automatizar tarefas comuns:

```bash
# Ver todos os comandos disponíveis
make help

# Setup completo (primeira vez)
make first-run

# Executar pipeline completo
make pipeline

# Gerenciar Docker
make docker-up       # Subir containers
make docker-down     # Parar containers
make docker-logs     # Ver logs

# Dashboard local
make dashboard

# Limpeza
make clean-buckets
```

### Executar localmente (fora do Docker)

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar pipeline
python -m scripts_pipeline.clean_buckets
python -m scripts_pipeline.insert_bronze
python -m scripts_pipeline.transform_silver
python -m scripts_pipeline.aggregate_gold

# Ver diagnóstico
python diagnose_data_flow.py

# Visualizar dashboard (após dados subirem para camada Gold)
streamlit run streamlit\dashboard.py
```

> **NOTA**: Se estiver usando Docker, o dashboard já está rodando automaticamente em http://localhost:8501

### Estrutura de Diretórios

```
Dataops/
├── dags/                    # Airflow DAGs
│   ├── sensors/            # Sensores customizados
│   └── env_config.py       # Configuração segura
├── scripts_pipeline/        # Scripts de transformação
│   ├── insert_bronze.py    # Ingestão
│   ├── transform_silver.py # Limpeza + NER
│   └── aggregate_gold.py   # Agregações
├── streamlit/              # Dashboard
│   └── dashboard.py
├── docker-compose.yml      # Orquestração
├── .env.example           # Template de configuração
└── docs/                   # Documentação completa
```

## Troubleshooting

### Container não inicia
```bash
docker-compose logs <container_name>
```

### Erro "Failed to resolve 'minio'"
Já corrigido! O sistema detecta automaticamente o ambiente.

### Label Studio - 401 Unauthorized
Certifique que está usando **Legacy Token**, não Access Token.

Ver mais em **[SETUP_COMPLETO.md](SETUP_COMPLETO.md) - Troubleshooting**

## Fluxo de Dados

1. **Ingestão**: Label Studio API → JSON estruturado → MinIO Bronze
2. **Transformação**: Bronze → Limpeza + NER extraction → MinIO Silver
3. **Agregação**: Silver → KPIs + Agregações → MinIO Gold
4. **Visualização**: Gold → Streamlit Dashboard

## Conceitos Aplicados

- **DataOps**: Orquestração, monitoramento, versionamento
- **Medallion Architecture**: Bronze (raw) → Silver (clean) → Gold (curated)
- **Event-Driven**: Processamento reativo a eventos
- **NER (Named Entity Recognition)**: Extração de entidades nomeadas
- **Containerização**: Docker, isolamento, portabilidade
- **Security**: Credenciais em variáveis de ambiente

## Decisões Arquiteturais e Limitações

### Decisões de Design

#### 1. Arquitetura Medallion (Bronze → Silver → Gold)
**Por quê?**
- **Separação de responsabilidades**: Cada camada tem propósito claro (raw, clean, curated)
- **Rastreabilidade**: Dados brutos preservados em Bronze para auditoria
- **Reprocessamento**: Possibilidade de reprocessar apenas camadas específicas
- **Evolução gradual**: Transformações incrementais facilitam debugging

**Implementação**:
- Bronze: Dados brutos em JSON do Label Studio
- Silver: Dados limpos em Parquet com validações e NER extraído
- Gold: Agregações pré-calculadas para dashboard

#### 2. Event-Driven com Sensores Deferráveis
**Por quê?**
- **Eficiência de recursos**: Sensores deferráveis liberam workers enquanto aguardam eventos
- **Processamento reativo**: Pipeline processa automaticamente quando novos dados chegam
- **Escalabilidade**: Não desperdiça recursos esperando ativamente por arquivos

**Implementação**:
- `S3KeySensor` em modo deferrable monitora bucket Bronze
- DAG acionada automaticamente quando novo arquivo JSON aparece
- Triggerer do Airflow gerencia sensores de forma assíncrona

#### 3. MinIO ao invés de S3 Real
**Por quê?**
- **Ambiente local**: Desenvolvimento e testes sem custos de cloud
- **Compatibilidade S3**: API 100% compatível, facilita migração futura
- **Controle total**: Dados permanecem localmente durante desenvolvimento

#### 4. Label Studio para Anotações NER
**Por quê?**
- **Interface visual**: Facilita anotação de entidades sem código
- **API robusta**: Integração programática para extração de dados
- **Open source**: Sem custos de licenciamento

#### 5. Streamlit para Dashboard
**Por quê?**
- **Rapidez de desenvolvimento**: Dashboard funcional em poucas linhas Python
- **Integração nativa**: Trabalha nativamente com Pandas e Plotly
- **Atualização em tempo real**: Cache TTL de 60s para dados sempre atualizados

### Limitações Conhecidas

#### 1. Escalabilidade de Volume
- **Limitação**: Pipeline projetado para ~500 registros, não testado em milhões
- **Impacto**: Pandas pode ter problemas de memória com datasets muito grandes
- **Mitigação futura**: Migrar para PySpark ou Dask para processamento distribuído

#### 2. Processamento Síncrono
- **Limitação**: Transformações executam sequencialmente (Bronze → Silver → Gold)
- **Impacto**: Não aproveita paralelização para múltiplos arquivos
- **Mitigação futura**: Implementar processamento paralelo com Celery ou Ray

#### 3. Validação de Dados
- **Implementação**: Validações de campos obrigatórios, tipos, dados não vazios
- **Dataset misto intencional**: 1000 registros (500 válidos + 500 inválidos)
  - Demonstra robustez do pipeline em lidar com dados problemáticos
  - Calcula métricas de qualidade (taxa de limpeza ~50%)
- **Limitação**: Validações simples, sem regras de negócio complexas
- **Mitigação futura**: Integrar Great Expectations para validações avançadas

#### 4. Ausência de Versionamento de Schema
- **Limitação**: Mudanças no schema Label Studio podem quebrar pipeline
- **Impacto**: Necessidade de ajustes manuais ao evoluir schema
- **Mitigação futura**: Implementar schema registry (Apache Avro/Protobuf)

#### 5. Monitoramento e Alertas Limitados
- **Limitação**: Logs básicos do Airflow, sem alertas proativos
- **Impacto**: Falhas podem passar despercebidas
- **Mitigação futura**: Integrar Prometheus + Grafana + alertas por email/Slack

## Validação de Resultados

### Como Validar Cada Camada do Pipeline

#### 1. Camada Bronze (Raw Data)

**Onde verificar**:
- **MinIO Console**: http://localhost:9001
  - Login com credenciais do `.env`
  - Navegue para bucket `bronze`
  - Deve conter arquivos `bronze_YYYYMMDD_HHMMSS.json`

**O que esperar**:
```json
[
  {
    "id": 1,
    "data": {"text": "Cliente João comprou notebook por R$ 2500"},
    "annotations": [...]
  }
]
```

**Validação via CLI**:
```bash
# Listar arquivos Bronze
python -c "from minio import Minio; from env_config import get_minio_config; cfg = get_minio_config(); client = Minio(endpoint=cfg['endpoint'], access_key=cfg['access_key'], secret_key=cfg['secret_key'], secure=False); print(list(client.list_objects('bronze')))"
```

#### 2. Camada Silver (Clean Data + NER)

**Onde verificar**:
- **MinIO Console**: http://localhost:9001
  - Bucket `silver`
  - Arquivos `silver_YYYYMMDD_HHMMSS.parquet`

**O que esperar**:
- Dados em formato Parquet
- Colunas NER extraídas: `cliente_ner`, `produto_ner`, `valor_ner`, etc.
- Registros inválidos removidos (validação de ID, data não vazia)

**Logs de validação**:
```bash
# Ver logs do Airflow para transform_silver
docker-compose logs airflow-scheduler | grep "transform_silver"

# Ou acessar Airflow UI
# http://localhost:8080 → DAGs → 00_event_driven_ingestion → Task transform_silver → Logs
```

**Saída esperada nos logs**:
```
Extraindo labels NER...
[EXTRAÍDO] cliente: 'joão silva'
[EXTRAÍDO] produto: 'notebook'
[EXTRAÍDO] valor: '2500'
Registros válidos: ~500/1000
Estatísticas: invalid_id=~250, invalid_data=~250
Taxa de limpeza: ~50%
```

#### 3. Camada Gold (Aggregations)

**Onde verificar**:
- **MinIO Console**: http://localhost:9001
  - Bucket `gold`
  - Arquivo `gold_analytics_YYYYMMDD_HHMMSS.parquet`

**O que esperar**:
- KPIs agregados por cliente, produto, região
- Valores totais, médias, contagens
- Dados prontos para dashboard

**Validação via script**:
```bash
python diagnose_data_flow.py
```

**Saída esperada**:
```
========================================
DIAGNÓSTICO COMPLETO DO PIPELINE
========================================

CAMADA BRONZE:
  Arquivos: 1
  Registros: 1000

CAMADA SILVER:
  Arquivos: 1
  Registros: ~500
  Taxa de retenção: ~50%

CAMADA GOLD:
  Arquivos: 1
  Registros: ~500
  KPIs calculados: 8
```

#### 4. Dashboard Streamlit

**Onde verificar**:
- **URL**: http://localhost:8501

**O que esperar**:
- **5 KPIs principais** no topo:
  - Receita Total
  - Total de Vendas
  - Clientes Únicos
  - Produtos Únicos
  - Ticket Médio

- **7 abas disponíveis**:
  1. Vendas: Gráficos de vendas por período
  2. Clientes: Top clientes, distribuição
  3. Produtos: Top produtos, categorias
  4. Geográfico: Vendas por região/cidade
  5. Pagamento: Métodos de pagamento
  6. Dados Brutos: Tabela completa exportável
  7. **Pipeline**: Monitoramento Bronze/Silver/Gold

**Aba Pipeline - O que validar**:
- Contagem de registros em cada camada
- Taxa de limpeza (% removidos Bronze → Silver)
- Gráfico de funil mostrando fluxo de dados
- Timestamps de última atualização de cada camada
- Taxas de retenção entre camadas

**Screenshot esperado**:
```
📂 Registros Bronze    ✅ Registros Silver    ⭐ Registros Gold    🧹 Taxa de Limpeza
     1000                    ~500                   ~500               ~50.0%
```

### Logs e Monitoramento

#### Airflow Logs

**Acessar via UI**:
1. http://localhost:8080
2. Login: `airflow` / `airflow`
3. DAGs → `00_event_driven_ingestion`
4. Graph View → Clique em qualquer task → Logs

**Acessar via Docker**:
```bash
# Logs do scheduler (onde DAGs executam)
docker-compose logs -f airflow-scheduler

# Logs de task específica
docker-compose logs airflow-scheduler | grep "insert_bronze"
```

**O que procurar nos logs**:
- `[INFO]`: Execuções bem-sucedidas
- `[ERROR]`: Falhas que precisam investigação
- `Task succeeded`: Task completada com sucesso
- `Poking for file`: Sensor aguardando arquivo

#### MinIO Logs

```bash
# Ver atividade de upload/download
docker-compose logs -f minio
```

#### Label Studio Logs

```bash
# Ver requisições API
docker-compose logs -f label-studio
```

## Reprodução do Cenário Event-Driven

Este guia mostra como testar o fluxo completo event-driven do pipeline.

**Vídeo demonstrativo**: https://drive.google.com/file/d/1MWBXpvQyZESNMVfSMm9WTBzPWJRU-WAS/view?usp=sharing

### Cenário: Pipeline Detecta Novo Arquivo e Processa Automaticamente

#### Passo 1: Preparar Ambiente

```bash
# Garantir que containers estão rodando
docker-compose ps

# Todos devem estar "Up (healthy)"
# Se não estiverem, execute:
docker-compose up -d

# Aguardar ~2min para inicialização completa
```

#### Passo 2: Limpar Buckets (Começar do Zero)

```bash
# Ativar ambiente Python
conda activate dataops

# Limpar todos os buckets
python -m scripts_pipeline.clean_buckets
```

**Saída esperada**:
```
Limpando bucket bronze...
Limpando bucket silver...
Limpando bucket gold...
Limpeza concluída!
```

#### Passo 3: Verificar DAG no Airflow

1. Acesse http://localhost:8080
2. Login: `airflow` / `airflow`
3. Localize DAG: `00_event_driven_ingestion`
4. **Ative a DAG** (toggle no canto esquerdo deve ficar azul)

#### Passo 4: Verificar Sensor em Execução

Na interface do Airflow:
1. Clique na DAG `00_event_driven_ingestion`
2. Graph View
3. Você deve ver task `wait_for_bronze_file` em estado **running** (verde claro)
4. Clique na task → Logs

**Logs esperados**:
```
[INFO] Poking for file s3://bronze/*.json
[INFO] Deferring task to triggerer
```

Isso significa que o sensor está **aguardando ativamente** por um arquivo JSON no bucket Bronze.

#### Passo 5: Triggerar o Evento (Upload para Bronze)

Agora vamos simular o evento: **upload de arquivo para Bronze**.

```bash
# Executar script de ingestão (simula aplicação enviando dados)
python -m scripts_pipeline.insert_bronze
```

**Saída esperada**:
```
Conectando ao Label Studio...
Extraindo dados do projeto 3...
Enviando para bucket bronze...
Arquivo bronze_20250115_143022.json criado com sucesso!
1000 registros enviados para Bronze
```

#### Passo 6: Observar Pipeline Event-Driven em Ação

**O que acontece automaticamente**:

1. **Sensor detecta arquivo** (~30s após upload):
   - Task `wait_for_bronze_file` muda para **SUCCESS** (verde)

2. **Pipeline executa sequencialmente**:
   - `transform_silver` inicia (amarelo → verde)
   - `aggregate_gold` inicia após Silver completar
   - `diagnose` executa verificação final

**Acompanhar em tempo real**:
1. Airflow UI → Graph View (atualiza a cada 30s)
2. Ou clique em "Auto-refresh" para atualizar automaticamente

**Tempo total esperado**: 2-4 minutos do upload até conclusão

#### Passo 7: Validar Resultado Final

**Via Airflow**:
- Todas as tasks devem estar verdes (SUCCESS)
- Task `diagnose` mostra estatísticas nos logs

**Via MinIO Console** (http://localhost:9001):
- Bronze: 1 arquivo JSON
- Silver: 1 arquivo Parquet
- Gold: 1 arquivo Parquet

**Via Dashboard** (http://localhost:8501):
- Aba "Pipeline" deve mostrar:
  - 1000 registros Bronze
  - ~500 registros Silver (metade removida por validação)
  - ~500 registros Gold
  - Taxa de limpeza ~50%

**Via CLI**:
```bash
python diagnose_data_flow.py
```

### Cenário Alternativo: Trigger Manual

Se quiser executar o pipeline **sem esperar pelo sensor**:

```bash
# Opção 1: Via Airflow UI
# Clique no botão "Play" (▶) na DAG

# Opção 2: Via CLI
docker-compose exec airflow-webserver airflow dags trigger 00_event_driven_ingestion
```

### Validação do Event-Driven

**Como confirmar que é event-driven de verdade?**

1. **Teste 1: Upload múltiplo**
   ```bash
   # Limpar buckets
   python -m scripts_pipeline.clean_buckets

   # Aguardar sensor detectar bucket vazio
   # Então fazer upload
   python -m scripts_pipeline.insert_bronze

   # Pipeline deve executar automaticamente
   ```

2. **Teste 2: Verificar modo deferrable**
   - Airflow logs do sensor devem mostrar: `Deferring task to triggerer`
   - Isso confirma que está usando deferrable mode (eficiente)

3. **Teste 3: Múltiplas execuções**
   - Após primeira execução, limpe buckets novamente
   - Faça novo upload
   - Nova execução da DAG deve ser triggerada automaticamente

## Licença

Este projeto foi desenvolvido como trabalho de conclusão de disciplina.

## Contribuindo

Contribuições são bem-vindas! Por favor:
1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## Suporte

- **Documentação**: Ver arquivos `.md` na raiz do projeto
- **Issues**: Abra uma issue no GitHub

---

**Desenvolvido usando Python, Airflow, Docker**
