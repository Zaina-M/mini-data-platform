#!/usr/bin/env bash
set -euo pipefail

required_vars=(AIRFLOW_DB_USER ANALYTICS_DB_USER METABASE_DB_USER)
for var_name in "${required_vars[@]}"; do
  if [ -z "${!var_name:-}" ]; then
    echo "Missing required environment variable: ${var_name}" >&2
    exit 1
  fi
done

wait_for() {
  local name="$1"
  local cmd="$2"
  local max="$3"

  for i in $(seq 1 "$max"); do
    if eval "$cmd" > /dev/null 2>&1; then
      echo "  + ${name} is healthy"
      return 0
    fi
    sleep 5
  done

  echo "  x ${name} FAILED after ${max} attempts"
  return 1
}

echo "Waiting for infrastructure..."
wait_for "MinIO" "curl -sf http://localhost:9000/minio/health/live" 60
wait_for "Airflow DB" "docker exec airflow-db pg_isready -U ${AIRFLOW_DB_USER}" 60
wait_for "Analytics DB" "docker exec analytics-db pg_isready -U ${ANALYTICS_DB_USER}" 60
wait_for "Metabase DB" "docker exec metabase-db pg_isready -U ${METABASE_DB_USER}" 60

echo "Waiting for airflow-init to complete..."
for i in $(seq 1 60); do
  STATUS=$(docker inspect --format='{{.State.Status}}' airflow-init 2>/dev/null || echo "unknown")
  if [ "$STATUS" = "exited" ]; then
    EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' airflow-init)
    if [ "$EXIT_CODE" = "0" ]; then
      echo "  + airflow-init completed successfully"
      break
    else
      echo "  x airflow-init exited with code ${EXIT_CODE}"
      docker logs airflow-init --tail 30
      exit 1
    fi
  fi
  echo "  airflow-init: ${STATUS} (attempt ${i}/60)"
  sleep 5
done

echo "Waiting for application services..."

echo "  Waiting for scheduler container..."
for i in $(seq 1 30); do
  SCHED_STATUS=$(docker inspect --format='{{.State.Status}}' airflow-scheduler 2>/dev/null || echo "not-found")
  if [ "$SCHED_STATUS" = "running" ]; then
    echo "  + Scheduler container is running"
    break
  fi
  echo "    scheduler status: ${SCHED_STATUS} (attempt ${i}/30)"
  sleep 5
done

FINAL_STATUS=$(docker inspect --format='{{.State.Status}}' airflow-scheduler 2>/dev/null || echo "unknown")
if [ "$FINAL_STATUS" != "running" ]; then
  echo "  x Scheduler container not running (status: ${FINAL_STATUS})"
  docker logs airflow-scheduler --tail 50
  exit 1
fi

echo "  Waiting for Airflow CLI..."
for i in $(seq 1 60); do
  if docker exec airflow-scheduler airflow db check > /dev/null 2>&1; then
    echo "  + Airflow database connection OK (attempt ${i})"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "  x Airflow database check failed after 60 attempts"
    docker compose ps
    docker logs airflow-scheduler --tail 100
    exit 1
  fi
  echo "    db check attempt ${i}/60..."
  sleep 5
done

echo "  Checking DAG parsing..."
if ! docker exec airflow-scheduler airflow dags list 2>&1; then
  echo "  x DAG listing failed"
  docker exec airflow-scheduler airflow dags list-import-errors 2>&1 || true
  exit 1
fi
echo "  + Airflow Scheduler is healthy"

wait_for "Metabase" "curl -sf http://localhost:3000/api/health" 60

echo "All services healthy"
docker compose ps
