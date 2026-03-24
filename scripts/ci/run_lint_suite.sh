#!/usr/bin/env bash
set -euo pipefail

flake8 airflow/dags/ data-generator/ tests/
black --check airflow/dags/ data-generator/ tests/
isort --check-only airflow/dags/ data-generator/ tests/
