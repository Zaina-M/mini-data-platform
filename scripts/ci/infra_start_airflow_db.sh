#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
	set -a
	. ./.env
	set +a
fi

DB_USER="${AIRFLOW_DB_USER:-airflow}"

docker compose up -d airflow-db
timeout 60 bash -c "until docker compose exec airflow-db pg_isready -U ${DB_USER}; do sleep 2; done"
