from invoke import task

COMPOSE = "docker compose"
WEB = "airflow-airflow-webserver-1"
SCHED = "airflow-airflow-scheduler-1"
MAIN_DAG_ID = "monitoramento_qualidade_ar"


@task
def up(c):
    """Sobe todos os serviços (Airflow, Postgres, etc.)"""
    c.run(f"{COMPOSE} up -d", pty=True)


@task
def down(c):
    """Derruba todos os serviços"""
    c.run(f"{COMPOSE} down", pty=True)


@task
def ps(c):
    """Mostra status dos containers"""
    c.run(f"{COMPOSE} ps", pty=True)


@task
def logs_web(c):
    """Mostra logs do webserver do Airflow"""
    c.run(f"{COMPOSE} logs {WEB} --tail=100 -f", pty=True)


@task
def logs_scheduler(c):
    """Mostra logs do scheduler do Airflow"""
    c.run(f"{COMPOSE} logs {SCHED} --tail=100 -f", pty=True)


@task
def airflow_init(c):
    """Inicializa o banco de metadados do Airflow"""
    c.run(f"{COMPOSE} run --rm {WEB} airflow db init", pty=True)


@task
def airflow_user(c):
    """Cria usuário admin padrão (airflow/airflow)"""
    c.run(
        f"""{COMPOSE} run --rm {WEB} \
airflow users create \
  --username airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password airflow""",
        pty=True,
    )


@task
def backfill_nov(c):
    """Roda backfill de novembro/2025 para a DAG principal"""
    c.run(
        f"""{COMPOSE} run --rm {WEB} \
airflow dags backfill {MAIN_DAG_ID} \
  -s 2025-11-01 \
  -e 2025-11-30""",
        pty=True,
    )


@task
def bash_web(c):
    """Abre um bash dentro do container do webserver"""
    c.run(f"{COMPOSE} exec {WEB} bash", pty=True)