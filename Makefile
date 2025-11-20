# ============================
# Makefile - Projeto DataOps FX
# ============================

PYTHON ?= python3
VENV ?= .venv

# ----------------------------
# Alvos "falsos" (não são arquivos)
# ----------------------------
.PHONY: help venv install lint format test tox airflow-init airflow-up airflow-down backfill-novembro dev

# ----------------------------
# Ajuda
# ----------------------------
help:
	@echo "Comandos disponíveis:"
	@echo "  make venv                -> cria ambiente virtual"
	@echo "  make install             -> instala dependências no venv"
	@echo "  make lint                -> roda flake8 + black --check nas DAGs"
	@echo "  make format              -> formata código com black"
	@echo "  make test                -> roda pytest na pasta tests/"
	@echo "  make tox                 -> roda tox (pipeline de testes)"
	@echo "  make airflow-init        -> inicializa o Airflow (migrations etc)"
	@echo "  make airflow-up          -> sobe o Airflow com docker-compose"
	@echo "  make airflow-down        -> derruba o Airflow"
	@echo "  make backfill-novembro   -> roda backfill da DAG de câmbio em novembro"
	@echo "  make dev                 -> fluxo completo: install + lint + test"

# ----------------------------
# Ambiente Python
# ----------------------------
venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	. $(VENV)/bin/activate && pip install -r requirements.txt

# ----------------------------
# Qualidade de código
# ----------------------------
lint:
	. $(VENV)/bin/activate && flake8 dags && black --check dags

format:
	. $(VENV)/bin/activate && black dags

test:
	. $(VENV)/bin/activate && pytest tests

tox:
	. $(VENV)/bin/activate && tox

dev: install lint test

# ----------------------------
# Airflow + Docker
# ----------------------------
airflow-init:
	docker compose -f airflow/docker-compose.yaml up airflow-init

airflow-up:
	docker compose -f airflow/docker-compose.yaml up -d

airflow-down:
	docker compose -f airflow/docker-compose.yaml down

# ----------------------------
# Backfill para o cenário do trabalho 
# ----------------------------
backfill-desde:
	@if [ -z "$(DESDE)" ]; then \
		echo "Uso: make backfill-desde DESDE=2025-09-01"; exit 1; \
	fi
	END=$$(date +"%Y-%m-%d"); \
	docker compose -f airflow/docker-compose.yaml exec airflow-webserver \
		airflow dags backfill monitoramento_cambio_anotacoes \
		-s $(DESDE) -e $$END

