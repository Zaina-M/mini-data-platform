#!/usr/bin/env bash
set -euo pipefail

DB_USER="${ANALYTICS_DB_USER:-}"
DB_NAME="${ANALYTICS_DB_NAME:-}"
MIN_ROWS="${MIN_ROWS:-1}"
SUCCESS_MESSAGE="${SUCCESS_MESSAGE:-STORAGE CHECK PASSED: rows loaded into PostgreSQL}"
FAILURE_MESSAGE="${FAILURE_MESSAGE:-STORAGE CHECK FAILED: no rows found in sales table}"

if [ -z "${DB_USER}" ] || [ -z "${DB_NAME}" ]; then
  echo "ANALYTICS_DB_USER and ANALYTICS_DB_NAME must be set" >&2
  exit 1
fi

ROW_COUNT=$(docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" \
  -t -c "SELECT COUNT(*) FROM sales;" | xargs)

echo "Rows in sales table: ${ROW_COUNT}"

if [ "${ROW_COUNT}" -ge "${MIN_ROWS}" ]; then
  echo "${SUCCESS_MESSAGE}"
  exit 0
fi

echo "${FAILURE_MESSAGE}"
exit 1
