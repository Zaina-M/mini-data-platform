#!/usr/bin/env bash
set -euo pipefail

DAG_ID="${1:-sales_data_pipeline}"
MAX_ATTEMPTS="${2:-60}"
SLEEP_SECONDS="${3:-10}"

if [ -z "${DAG_ID}" ]; then
  echo "DAG_ID is required" >&2
  exit 1
fi

echo "Polling DAG run status via CLI..."
for i in $(seq 1 "${MAX_ATTEMPTS}"); do
  STATE=$(docker exec airflow-scheduler \
    airflow dags list-runs -d "${DAG_ID}" \
    -o plain --no-backfill 2>/dev/null \
    | head -5 | grep -oE '(success|failed|running|queued)' \
    | head -1 || echo "unknown")

  echo "  attempt ${i}/${MAX_ATTEMPTS} - state: ${STATE}"

  if [ "${STATE}" = "success" ]; then
    echo "PROCESSING CHECK PASSED: DAG completed successfully"
    exit 0
  fi

  if [ "${STATE}" = "failed" ]; then
    echo "DAG run failed! Dumping scheduler logs..."
    docker compose logs airflow-scheduler --tail 80
    exit 1
  fi

  sleep "${SLEEP_SECONDS}"
done

echo "DAG run timed out"
docker compose logs airflow-scheduler --tail 80
exit 1
