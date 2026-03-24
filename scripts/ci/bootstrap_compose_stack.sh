#!/usr/bin/env bash
set -euo pipefail

docker compose down -v --remove-orphans 2>/dev/null || true
docker volume prune -f

mkdir -p airflow/logs/scheduler airflow/logs/dag_processor_manager airflow/logs/pipeline
mkdir -p airflow/plugins airflow/config
chmod -R 777 airflow/logs

docker compose up -d --build
echo "Services starting..."
