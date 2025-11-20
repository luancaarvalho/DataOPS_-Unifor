from invoke import task


@task
def lint(c):
    """Roda flake8 + black --check nas DAGs."""
    c.run("flake8 dags")
    c.run("black --check dags")


@task
def format(c):
    """Formata código com black."""
    c.run("black dags")


@task
def test(c):
    """Roda pytest."""
    c.run("pytest tests")


@task
def airflow_up(c):
    """Sobe o Airflow via docker-compose."""
    c.run("docker compose -f docker-compose-airflow.yml up -d", pty=True)


@task
def airflow_down(c):
    """Derruba o Airflow."""
    c.run("docker compose -f docker-compose-airflow.yml down", pty=True)


@task
def backfill_meses(c):
    """Roda backfill de meses da DAG de câmbio."""
    c.run(
        "docker compose -f docker-compose-airflow.yml exec airflow-webserver "
        "airflow dags backfill monitoramento_cambio_anotacoes "
        "-s 2025-09-01 -e 2025-11-01",
        pty=True,
    )
