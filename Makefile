# Makefile - DataOps Pipeline

.PHONY: help setup install init up down restart logs clean pipeline run dashboard test lint format

PYTHON := python
CONDA_ENV := dataops

.DEFAULT_GOAL := help

help:
	@echo "DataOps Pipeline - Comandos:"
	@echo ""
	@echo "  make init       - Configurar .env automaticamente (primeira vez)"
	@echo "  make setup      - Criar ambiente conda + instalar dependências"
	@echo "  make install    - Instalar dependências"
	@echo ""
	@echo "  make up         - Subir containers Docker"
	@echo "  make down       - Parar containers"
	@echo "  make restart    - Reiniciar containers"
	@echo "  make logs       - Ver logs dos containers"
	@echo ""
	@echo "  make clean      - Limpar buckets MinIO"
	@echo "  make run        - Executar pipeline completo"
	@echo "  make dashboard  - Abrir dashboard Streamlit"
	@echo ""
	@echo "  make test       - Rodar testes"
	@echo "  make lint       - Verificar código"
	@echo ""

# Inicialização
init:
ifeq (,$(wildcard .env))
	@echo "Criando .env com credenciais padrão..."
	@cp .env.example .env
	@echo "Pronto! Credenciais:"
	@echo "  MinIO: dataops_admin / DataOps2025!SecurePassword"
	@echo "  Airflow: airflow / airflow"
	@echo "  Label Studio: admin@localhost.com / 123456"
	@echo ""
	@echo "IMPORTANTE: Configure o LABELSTUDIO_TOKEN após subir os containers."
else
	@echo ".env já existe. Pulando..."
endif

# Setup
setup:
	conda create -n $(CONDA_ENV) python=3.10 -y
	conda run -n $(CONDA_ENV) pip install uv
	conda run -n $(CONDA_ENV) uv sync --directory environments
	@echo "Pronto! Execute: conda activate $(CONDA_ENV)"

install:
	uv sync --directory environments

# Docker
up: init
	docker-compose up -d
	@echo ""
	@echo "Containers iniciados! Aguarde 2-3 minutos para inicialização completa."
	@echo ""
	@echo "Serviços disponíveis:"
	@echo "  Airflow:      http://localhost:8080 (airflow/airflow)"
	@echo "  Label Studio: http://localhost:8001 (admin@localhost.com/123456)"
	@echo "  MinIO:        http://localhost:9001 (dataops_admin/DataOps2025!SecurePassword)"
	@echo "  Dashboard:    http://localhost:8501"
	@echo ""
	@echo "Verificar status: make logs"

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

# Pipeline
clean:
	$(PYTHON) -m scripts_pipeline.clean_buckets

bronze:
	$(PYTHON) -m scripts_pipeline.insert_bronze

silver:
	$(PYTHON) -m scripts_pipeline.transform_silver

gold:
	$(PYTHON) -m scripts_pipeline.aggregate_gold

diagnose:
	$(PYTHON) diagnose_data_flow.py

run: bronze silver gold diagnose
	@echo "Pipeline executado!"

pipeline: run

# Dashboard
dashboard:
	@echo "Dashboard: http://localhost:8501"
	streamlit run streamlit/dashboard.py

# Dev
test:
	pytest tests/ -v

lint:
	ruff check .

format:
	black .
