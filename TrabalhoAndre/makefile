.PHONY: up down init reset logs

up:
	cd infra && docker compose up -d

down:
	cd infra && docker compose down

init:
	# inicializa DB e cria usuário (no webserver entrypoint já tenta criar)
	cd infra && docker compose up --no-start postgres && docker compose start postgres

reset: down
	rm -rf infra/postgres-data infra/minio-data || true
	make up

logs:
	docker compose -f infra/docker-compose.yml logs -f
