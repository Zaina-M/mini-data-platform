#!/usr/bin/env bash
set -euo pipefail

docker compose up -d airflow-db
timeout 60 bash -c 'until docker compose exec airflow-db pg_isready -U airflow; do sleep 2; done'
