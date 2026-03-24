#!/usr/bin/env bash
set -euo pipefail

cat > .env <<EOF
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
EOF
