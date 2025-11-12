<h1 align="center">DataOPS — Pipeline COVID-19 (Airflow + PostgreSQL + Grafana)</h1>

<p align="center">
  Pipeline <b>end-to-end</b> orquestrado em <b>Apache Airflow</b> para ingestão, transformação, qualidade e visualização de dados de COVID-19
  (fonte: <code>brasil.io</code>), com armazenamento em <b>PostgreSQL</b> e dashboard pronto em <b>Grafana</b>.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Airflow-2.10-blue" />
  <img src="https://img.shields.io/badge/PostgreSQL-13-336791" />
  <img src="https://img.shields.io/badge/Grafana-Latest-orange" />
  <img src="https://img.shields.io/badge/Docker-Compose-green" />
</p>

<hr/>

<h2>O que este projeto entrega</h2>
<ul>
  <li><b>Ingestão</b> via API pública <code>https://brasil.io/covid19/cities/cases/</code>.</li>
  <li><b>Transformação</b> com Pandas + cálculo de média móvel de 7 dias.</li>
  <li><b>Qualidade</b> (checagem de nulos e valores negativos).</li>
  <li><b>Armazenamento</b> em PostgreSQL (tabela <code>covid_cases</code>).</li>
  <li><b>Dashboard Grafana</b> provisionado automaticamente (datasource + dashboard).</li>
  <li><b>Orquestração</b> e rastreabilidade com Apache Airflow.</li>
</ul>

<hr/>

<h2>Arquitetura (visão geral)</h2>

<pre>
[brasil.io] --ingestão--> [CSV bruto]
                         \---> [Transformação + Qualidade] --load--> [PostgreSQL] --(datasource)--> [Grafana]
                                                     ^
                                                     |
                                                  [Airflow DAG]
</pre>

<hr/>

<h2>Estrutura (trecho relevante)</h2>

<pre>
dataOps-Unifor/
└─ airflow/
   ├─ dags/
   │  └─ covid_pipeline_dag.py
   ├─ include/
   │  ├─ ingestion.py
   │  ├─ transform.py
   │  └─ load_postgres.py
   ├─ grafana/
   │  ├─ dashboards/
   │  │  └─ covid_dashboard.json
   |  |  └─ monitoramento.json
   │  └─ provisioning/
   │     ├─ datasources.yml
   │     └─ dashboards/
   │        └─ covid-dashboard.yml
   ├─ data/
   │  ├─ raw/               (csv bruto gerado pela ingestão)
   │  └─ processed/         (csv transformado)
   ├─ docker-compose.yaml
   └─ Dockerfile
</pre>

<hr/>

<h2>Pré-requisitos</h2>
<ul>
  <li>Docker Desktop (ou Docker Engine) + Docker Compose</li>
  <li>Portas livres: <code>8080</code> (Airflow), <code>3000</code> (Grafana)</li>
</ul>

<hr/>

<h2>Como subir o ambiente</h2>

<h3>1) Clone o repositório</h3>

<pre><code>git clone &lt;seu-fork-ou-repo&gt;
cd DataOPS_-Unifor/trabalho-final-dataOps/airflow
</code></pre>

<h3>2) Suba os containers</h3>

<p>Com <b>Docker Compose</b>:</p>
<pre><code>docker compose up -d --build
</code></pre>

<p>Ou, se o repositório tiver <code>Makefile</code> com alvo <code>up</code>:</p>
<pre><code>make up
</code></pre>

<h3>3) Acesse as UIs</h3>

<ul>
  <li><b>Airflow:</b> <code>http://localhost:8080</code> (usuário: <code>airflow</code>, senha: <code>airflow</code>)</li>
  <li><b>Grafana:</b> <code>http://localhost:3000</code> (usuário: <code>admin</code>, senha: <code>admin</code>)</li>
</ul>

<hr/>

<h2>Executando o pipeline</h2>

<ol>
  <li>Abra o Airflow e <b>ative</b> (unpause) a DAG <code>covid_pipeline_dag</code>.</li>
  <li><b>Trigger</b> (play) para executar imediatamente.</li>
  <li>O fluxo executa, na ordem:
    <ul>
      <li><code>wait_for_data</code> (sensor local) — aguarda o CSV bruto <code>/opt/airflow/data/raw/covid_data.csv</code>. No primeiro run, o arquivo será gerado pela ingestão e, nas próximas execuções, o sensor passa direto se o arquivo já existir.</li>
      <li><code>ingest_data</code> — baixa dados da API e salva CSV bruto.</li>
      <li><code>validate_data</code> — checa nulos e valores negativos.</li>
      <li><code>transform_and_load</code> — transforma e grava na tabela <code>covid_cases</code> (PostgreSQL).</li>
      <li><code>notify_grafana</code> — registra que os dados já estão disponíveis para o dashboard.</li>
    </ul>
  </li>
  <li>No Grafana, o dashboard <b>“Monitoramento COVID Ceará”</b> já estará provisionado:
    <ul>
      <li>Datasource: <b>covid_postgres</b> (conectado ao banco <code>airflow</code>, usuário <code>airflow</code>, senha <code>airflow</code>)</li>
      <li>Dashboard: em <i>Dashboards → COVID Dashboards</i></li>
    </ul>
  </li>
</ol>

<hr/>

<h2>Tabela criada</h2>

<ul>
  <li><code>covid_cases</code> (no banco <code>airflow</code>)</li>
</ul>

<p>Colunas principais:</p>
<ul>
  <li><code>city</code>, <code>date</code>, <code>confirmed</code>, <code>deaths</code>, <code>rolling_avg</code></li>
</ul>

<p>Exemplo para inspecionar via psql:</p>

<pre><code>docker exec -it airflow-postgres-1 psql -U airflow -d airflow -c "SELECT city, date, confirmed, deaths FROM covid_cases LIMIT 10;"
</code></pre>

<hr/>

<h2>Dashboard (Grafana)</h2>

<ul>
  <li>Datasource provisionado em <code>grafana/provisioning/datasources.yml</code> (uid: <code>covid_postgres</code>).</li>
  <li>Dashboard salvo em <code>grafana/dashboards/covid_dashboard.json</code> e carregado por <code>grafana/provisioning/dashboards/covid-dashboard.yml</code>.</li>
  <li>Painel principal: <b>Evolução de Casos Confirmados por Cidade</b> (série temporal com linhas, interpolação suave e legenda tabular).</li>
</ul>

<hr/>

<h2>Como “forçar” um novo run</h2>

<ol>
  <li>No Airflow, clique em <b>Trigger DAG</b> na <code>covid_pipeline_dag</code>.</li>
  <li>Ou rode a ingestão manualmente dentro do container:
    <pre><code>docker exec -it airflow-airflow-scheduler-1 python /opt/airflow/include/ingestion.py</code></pre>
  </li>
</ol>

<hr/>

<h2>Parar e limpar</h2>

<p>Parar containers:</p>
<pre><code>docker compose down
</code></pre>

<p>Parar e <b>remover volumes</b> (inclui dados do Postgres e estado do Grafana – cuidado):</p>
<pre><code>docker compose down -v
</code></pre>

<hr/>

<h2>Troubleshooting rápido</h2>

<ul>
  <li><b>Sensor de arquivo falhando</b>: o projeto usa um sensor local customizado (não depende do <code>fs_default</code>). Certifique-se de que o caminho no DAG é <code>/opt/airflow/data/raw/covid_data.csv</code> e que o volume <code>../data:/opt/airflow/data</code> está montado no <code>docker-compose.yaml</code>.</li>
  <li><b>Falha ao conectar no Postgres</b>: garanta que o serviço <code>postgres</code> esteja <i>healthy</i> e que a string do <code>create_engine</code> (em <code>include/load_postgres.py</code>) é
    <code>postgresql+psycopg2://airflow:airflow@postgres:5432/airflow</code>.</li>
  <li><b>Dashboard vazio</b>: verifique o <i>time range</i> do painel no Grafana e se a tabela <code>covid_cases</code> possui registros para o período selecionado.</li>
</ul>

<hr/>

<h2>Tecnologias</h2>

<table>
  <tr><td>Orquestração</td><td>Apache Airflow 2.10</td></tr>
  <tr><td>Ingestão</td><td>Requests</td></tr>
  <tr><td>Transformação</td><td>Pandas</td></tr>
  <tr><td>Storage</td><td>PostgreSQL</td></tr>
  <tr><td>BI</td><td>Grafana</td></tr>
  <tr><td>Infra</td><td>Docker Compose</td></tr>
</table>

<hr/>

<h2>Licença</h2>
<p>Projeto acadêmico — uso educacional.</p>
