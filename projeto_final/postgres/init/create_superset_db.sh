#!/bin/bash
set -e

# Aguarda o Postgres inicializar
sleep 3

# Testa se o banco existe
DB_EXISTS=$(psql -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='superset'")

if [ "$DB_EXISTS" = "1" ]; then
    echo "📌 Banco 'superset' já existe — nada a fazer."
else
    echo "🆕 Criando banco 'superset'..."
    psql -U "$POSTGRES_USER" -c "CREATE DATABASE superset OWNER airflow;"
fi