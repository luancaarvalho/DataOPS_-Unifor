### Projeto desemvolvido por:
## Nome / Matricula
* Marcos Aurelio Mendes Oliveira - 2519887
* Evellen Thais Gomes Silva - 2518889

# Projeto de Pipeline de Dados com Airflow, Label Studio e Grafana

Este projeto demonstra um pipeline de dados de ponta a ponta (end-to-end) utilizando ferramentas de orquestração, armazenamento, anotação de dados e visualização. O fluxo de trabalho extrai dados da taxa Selic da API do Banco Central, processa-os, armazena-os, envia para anotação e, finalmente, visualiza os resultados.

## Arquitetura

O ambiente é totalmente containerizado usando Docker e Docker Compose, e é composto pelos seguintes serviços:

*   **Apache Airflow**: Orquestrador de pipelines para agendar e executar as tarefas (DAGs).
*   **MinIO**: Armazenamento de objetos compatível com S3, usado como Data Lake (camada Bronze).
*   **MySQL**: Banco de dados relacional para armazenar os dados processados (camada Silver).
*   **Label Studio**: Ferramenta de anotação de dados para classificação manual.
*   **Grafana**: Plataforma de visualização e monitoramento de dados.
*   **PostgreSQL**: Metastore do Airflow.
*   **Redis**: Message broker para o CeleryExecutor do Airflow.

## Pré-requisitos

*   [Docker](https://www.docker.com/get-started)
*   [Docker Compose](https://docs.docker.com/compose/install/)

## 1. Configuração do Ambiente

Siga os passos abaixo para configurar e iniciar todos os serviços.

### 1.1. Iniciar os Serviços

Na raiz do projeto, execute o seguinte comando para construir e iniciar todos os containers em modo detached (-d):

```bash
docker-compose up -d
```

Aguarde alguns minutos para que todos os serviços, especialmente o Airflow, sejam iniciados completamente.

### 1.2. Configurar o MinIO (Data Lake)

1.  Acesse a interface do MinIO em **http://localhost:9001**.
2.  Faça login com as credenciais:
   *   **Username**: `minioadmin`
   *   **Password**: `minioadmin`
3.  No menu à esquerda, vá para **Buckets** e clique em **Create Bucket**.
4.  Crie um bucket com o nome `bronze`. Este bucket será usado pela primeira DAG para armazenar os dados brutos.

### 1.3. Configurar o Label Studio

1.  Acesse a interface do Label Studio em **http://localhost:8085**.
2.  Crie uma nova conta de administrador ou faça login com as credenciais padrão (se for o primeiro acesso):
    *   **Username**: `admin`
    *   **Password**: `admin`
3.  **Crie um projeto**:
    *   Clique em **Create Project**.
    *   Dê um nome ao projeto (ex: "Classificação Selic").
    *   Na aba **Labeling Setup**, clique em **Custom template** e cole o seguinte código XML para definir a interface de anotação.
        ```xml
        <View>
          <Text name="text" value="$text"/>
          <Choices name="category" toName="text">
            <Choice value="Acima da Meta"/>
            <Choice value="Abaixo da Meta"/>
          </Choices>
        </View>
        ```
    *   Salve o projeto. Anote o **ID do projeto** que aparece na URL (ex: `http://localhost:8085/projects/1`). O primeiro projeto geralmente terá o ID `1`.
4.  **Obtenha a Chave da API**:
    *   No canto superior direito, clique no ícone do seu perfil e vá para **Account & Settings**.
    *   Copie a sua **Access Token**.

> **⚠️ Atenção:** A DAG `inserting_data_on_labelstudio_dag` possui o token e o ID do projeto fixos no código. Para um ambiente de produção, é **altamente recomendado** armazenar esses valores como Variáveis ou Conexões do Airflow para maior segurança e flexibilidade.

### 1.4. Configurar o Grafana

1.  Acesse a interface do Grafana em **http://localhost:3000**.
2.  Faça login com as credenciais padrão:
    *   **Username**: `admin`
    *   **Password**: `admin`
3.  **Adicione a fonte de dados (Data Source)**:
    *   No menu à esquerda, vá para **Connections** -> **Add new connection**.
    *   Procure e selecione **MySQL**.
    *   Preencha os detalhes da conexão:
        *   **Host**: `mysql:3306`
        *   **Database**: `selic_data`
        *   **User**: `root`
        *   **Password**: `rootpassword`
    *   Clique em **Save & test**.
4.  **Importe o Dashboard**:
    *   No menu à esquerda, vá para **Dashboards** -> **New** -> **Import**.
    *   Importe o arquivo `dashboard.json` (se disponível no projeto) ou crie seus próprios painéis para visualizar os dados da tabela `data` do MySQL.

## 2. Executando o Pipeline

1.  Acesse a interface do Airflow em **http://localhost:8080**.
2.  Faça login com as credenciais:
    *   **Username**: `airflow`
    *   **Password**: `airflow`
3.  Na lista de DAGs, ative e execute as DAGs na seguinte ordem:
    1.  **`input_data_dag`**: Esta DAG busca os dados da API do Banco Central e os salva como arquivos `.csv` no bucket `bronze` do MinIO.
    2.  **`process_data_dag`**: Esta DAG lê os arquivos do bucket `bronze`, os processa e os insere no banco de dados MySQL.
    3.  **`inserting_data_on_labelstudio_dag`**: Esta DAG busca os dados do MySQL e os envia como tarefas para o projeto criado no Label Studio.

## 3. Anotação e Visualização

1.  **Anotar Dados**:
    *   Volte para o seu projeto no **Label Studio** (http://localhost:8085).
    *   Clique em **Label All Tasks** e comece a classificar os dados como "Acima da Meta" ou "Abaixo da Meta".
2.  **Verificar no Grafana**:
    *   Acesse seu dashboard no **Grafana** (http://localhost:3000).
    *   Os painéis configurados devem refletir os dados que foram processados e, eventualmente, as anotações feitas (se o dashboard for configurado para isso).

