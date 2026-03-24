#!/usr/bin/env bash
set -euo pipefail

pytest tests/ -v --tb=short --cov=airflow/dags --cov-report=term-missing
