#!/usr/bin/env bash
set -euo pipefail

echo "=== Docker container status ==="
docker compose ps -a
echo "=== metabase-db logs ==="
docker compose logs metabase-db --tail 50
echo "=== analytics-db logs ==="
docker compose logs analytics-db --tail 50
echo "=== airflow-scheduler logs ==="
docker compose logs airflow-scheduler --tail 100
echo "=== airflow-webserver logs ==="
docker compose logs airflow-webserver --tail 50
echo "=== metabase logs ==="
docker compose logs metabase --tail 50
