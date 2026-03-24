#!/usr/bin/env bash
set -euo pipefail

mkdir -p airflow/logs/scheduler airflow/logs/dag_processor_manager airflow/logs/pipeline
mkdir -p airflow/plugins airflow/config
chmod -R 777 airflow/logs
