# Makefile for managing Airflow docker-compose stack

COMPOSE := docker compose -f docker-compose.yaml

.PHONY: up down reset build

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans && rm -rf logs/dag*
