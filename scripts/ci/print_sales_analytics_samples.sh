#!/usr/bin/env bash
set -euo pipefail

DB_USER="${ANALYTICS_DB_USER:-}"
DB_NAME="${ANALYTICS_DB_NAME:-analytics}"

if [ -z "${DB_USER}" ]; then
  echo "ANALYTICS_DB_USER must be set" >&2
  exit 1
fi

echo "=== Sample data ==="
docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c \
  "SELECT order_id, product_name, quantity, unit_price, total_amount, country FROM sales LIMIT 5;"

echo "=== Daily sales summary ==="
docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c \
  "SELECT * FROM daily_sales_summary LIMIT 5;"

echo "=== Product performance ==="
docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c \
  "SELECT * FROM product_performance LIMIT 5;"

echo "=== Country analysis ==="
docker exec analytics-db psql \
  -U "${DB_USER}" \
  -d "${DB_NAME}" -c \
  "SELECT * FROM country_analysis LIMIT 5;"
