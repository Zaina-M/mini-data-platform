#!/usr/bin/env bash
set -euo pipefail

DB_USER="${ANALYTICS_DB_USER:-}"
DB_NAME="${ANALYTICS_DB_NAME:-analytics}"

if [ -z "${DB_USER}" ]; then
  echo "ANALYTICS_DB_USER must be set" >&2
  exit 1
fi

docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c "
  SELECT table_name FROM information_schema.tables
  WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
"

docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c "
  SELECT table_name FROM information_schema.views
  WHERE table_schema = 'public';
"
