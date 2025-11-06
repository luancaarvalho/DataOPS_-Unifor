# Configuracao Git-Sync para Apache Airflow

Este documento descreve como configurar e ativar o servico git-sync para sincronizacao automatica de DAGs do repositorio Git no Apache Airflow 2.10.0.

## Status Atual

O servico git-sync esta COMENTADO no arquivo `docker-compose.yaml`. O Airflow esta usando o volume local `../dags` para carregar os DAGs.

## O que e Git-Sync?

Git-sync e um container sidecar que sincroniza automaticamente um repositorio Git em um volume compartilhado. Ele monitora o repositorio em intervalos regulares e puxa as atualizacoes, permitindo que o Airflow detecte novos DAGs sem necessidade de reiniciar os containers.

## Autenticacao com GitHub

O git-sync suporta HTTPS Basic Authentication, onde:
- Username: `oauth2` (valor padrao para GitHub com Personal Access Token)
- Password: Seu Personal Access Token (PAT) do GitHub

### Criando um Personal Access Token

1. Acesse GitHub Settings > Developer Settings > Personal Access Tokens > Tokens (classic)
2. Clique em "Generate new token (classic)"
3. Selecione o escopo: `repo` (acesso completo ao repositorio)
4. Copie o token gerado (ele so sera exibido uma vez)

## Configuracao

### Passo 1: Configurar Variaveis de Ambiente

Edite o arquivo `.env` no diretorio `aula_01/airflow/`:

```bash
# GitHub Git-Sync Configuration
GITHUB_TOKEN=seu_personal_access_token_aqui
GITHUB_REPO=https://github.com/luancaarvalho/DataOPS_-Unifor.git
GITHUB_USERNAME=oauth2
GIT_SYNC_BRANCH=main
```

Importante: Nunca commite o arquivo `.env` com credenciais reais. Use `.env.example` como template.

### Passo 2: Descomentar o Servico Git-Sync

No arquivo `docker-compose.yaml`, localize a secao do servico git-sync (linha ~292) e descomente todas as linhas:

Antes:
```yaml
  # git-sync:
  #   image: registry.k8s.io/git-sync/git-sync:v4.2.1
  #   environment:
  #     GITSYNC_REPO: ${GITHUB_REPO:-https://github.com/...}
```

Depois:
```yaml
  git-sync:
    image: registry.k8s.io/git-sync/git-sync:v4.2.1
    environment:
      GITSYNC_REPO: ${GITHUB_REPO:-https://github.com/...}
```

### Passo 3: Ajustar Volumes do Airflow

No mesmo arquivo `docker-compose.yaml`, na secao `x-airflow-common` > `volumes` (linha ~76):

1. Descomente a variavel de ambiente:
```yaml
AIRFLOW__CORE__DAGS_FOLDER: '/git/repo/aula_01/dags'
```

2. Ajuste os volumes:

Comente:
```yaml
# - ../dags:/opt/airflow/dags
```

Descomente:
```yaml
- git-sync-dags:/git:ro
```

### Passo 4: Iniciar os Servicos

```bash
cd aula_01/airflow
docker-compose up -d
```

Verifique os logs do git-sync:
```bash
docker-compose logs -f git-sync
```

Voce devera ver mensagens indicando a clonagem e sincronizacao do repositorio.

## Parametros de Configuracao

O servico git-sync no docker-compose.yaml utiliza os seguintes parametros:

| Parametro | Descricao | Valor Padrao |
|-----------|-----------|--------------|
| `GITSYNC_REPO` | URL do repositorio Git (HTTPS) | Configurado via .env |
| `GITSYNC_USERNAME` | Usuario para autenticacao (usar "oauth2" para PAT) | oauth2 |
| `GITSYNC_PASSWORD` | Password ou Personal Access Token | Configurado via .env |
| `GITSYNC_BRANCH` | Branch a ser sincronizado | main |
| `GITSYNC_ROOT` | Diretorio raiz para clonagem | /git |
| `GITSYNC_DEST` | Nome do diretorio de destino | repo |
| `GITSYNC_PERIOD` | Intervalo de sincronizacao | 10s |
| `GITSYNC_ONE_TIME` | Sincronizar apenas uma vez e sair | false |

## Estrutura de Diretorios

Quando git-sync estiver ativo, a estrutura sera:

```
/git/
  └── repo/                    <- Clone do repositorio
      └── aula_01/
          └── dags/           <- DAGs sincronizados
              ├── airbyte_test.py
              ├── weather_dag.py
              └── yfinance_dag.py
```

O Airflow lera os DAGs de: `/git/repo/aula_01/dags`

## Verificacao

1. Acesse a interface do Airflow: http://localhost:8080
2. Login: airflow / airflow
3. Verifique se os DAGs aparecem na lista principal
4. Faca uma modificacao em um DAG no GitHub
5. Aguarde ate 10 segundos (GITSYNC_PERIOD)
6. Atualize a pagina do Airflow para ver as mudancas

## Troubleshooting

### Git-sync nao esta clonando o repositorio

Verifique:
- Token do GitHub esta correto no arquivo `.env`
- Token possui permissoes de leitura no repositorio
- URL do repositorio esta correta

```bash
docker-compose logs git-sync
```

### DAGs nao aparecem no Airflow

Verifique:
- `AIRFLOW__CORE__DAGS_FOLDER` aponta para o caminho correto
- Volume `git-sync-dags` esta montado em `/git:ro` nos containers Airflow
- Caminho completo deve ser: `/git/repo/aula_01/dags`

```bash
docker-compose exec airflow-scheduler ls -la /git/repo/aula_01/dags
```

### Erro de permissoes

O git-sync cria arquivos com permissoes especificas. Se houver problemas de permissao:

```bash
docker-compose down -v
docker-compose up -d
```

## Referencias

- Git-Sync Documentation: https://github.com/kubernetes/git-sync
- Apache Airflow Docker: https://airflow.apache.org/docs/apache-airflow/2.10.0/howto/docker-compose/index.html
- GitHub Personal Access Tokens: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token

## Desativando Git-Sync

Para desativar e voltar ao modo local:

1. Comente o servico `git-sync` no docker-compose.yaml
2. Comente `AIRFLOW__CORE__DAGS_FOLDER` na secao environment
3. Comente o volume `- git-sync-dags:/git:ro`
4. Descomente o volume `- ../dags:/opt/airflow/dags`
5. Reinicie os containers: `docker-compose restart`

