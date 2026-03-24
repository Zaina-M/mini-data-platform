#!/usr/bin/env bash
set -euo pipefail

DAG_ID="${1:-sales_data_pipeline}"

if [ -z "${DAG_ID}" ]; then
  echo "DAG_ID is required" >&2
  exit 1
fi

docker exec airflow-scheduler airflow dags unpause "${DAG_ID}"
echo "DAG unpaused: ${DAG_ID}"
docker exec airflow-scheduler airflow dags trigger "${DAG_ID}"
echo "DAG triggered: ${DAG_ID}"
