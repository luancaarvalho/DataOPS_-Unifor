# ============================
# Makefile - Projeto DataOps FX
# ============================

PYTHON ?= python3
VENV ?= .venv

# Carrega variáveis do .env (se existir)
-include .env
export

COMPOSE = airflow/docker-compose.yaml

# ======================================================================
# Subir / derrubar o Airflow
# ======================================================================

up:
	docker compose -f $(COMPOSE) up -d

down:
	docker compose -f $(COMPOSE) down

logs:
	docker compose -f $(COMPOSE) logs -f

# ======================================================================
# Inicializar Airflow (cria banco, inicializa metadados)
# ======================================================================

init-airflow:
	docker compose -f $(COMPOSE) exec airflow-webserver airflow db init
	docker compose -f $(COMPOSE) exec airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin || true

# ======================================================================
# Criar conexão Postgres no Airflow
# ======================================================================

create-conn-postgres:
	docker compose -f $(COMPOSE) exec airflow-webserver \
		airflow connections add 'postgres_default' \
			--conn-type 'postgres' \
			--conn-host '$(POSTGRES_HOST)' \
			--conn-login '$(POSTGRES_USER)' \
			--conn-password '$(POSTGRES_PASSWORD)' \
			--conn-port '$(POSTGRES_PORT)' \
			--conn-schema '$(POSTGRES_DB)' || true

# ======================================================================
# Backfill manual
# ======================================================================

backfill-desde:
	@if [ -z "$(DESDE)" ]; then \
		echo "Uso: make backfill-desde DESDE=2025-08-01"; exit 1; \
	fi
	END=$$(date +"%Y-%m-%d"); \
	docker compose -f $(COMPOSE) exec airflow-webserver \
		airflow dags backfill monitoramento_cambio_anotacoes \
		-s $(DESDE) -e $$END

# ======================================================================
# Rodar Testes (tox e pytest)
# ======================================================================

test:
	tox || pytest tests/ --maxfail=1 --disable-warnings

# ======================================================================
# Limpar tudo (containers + volumes)
# ======================================================================

clean:
	docker compose -f $(COMPOSE) down -v
	rm -rf logs/ || true
	rm -rf airflow/airflow.db || true

